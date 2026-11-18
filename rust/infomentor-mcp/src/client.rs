//! packages/infomentor-mcp/src/client.ts's `InfoMentorClient`: one queue of account operations per
//! MCP connection. Each read holds the store lock, proves the session on the parent page, and
//! writes changed cookies back before the next read.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, PoisonError};
use std::time::Duration;

use family_store::{Cancel, KeyProvider};
use serde_json::{Map, Value, json};
use tokio::sync::{Mutex, watch};

use crate::collection::{Source, collect_updates};
use crate::error::{Code, Fail, Result};
use crate::html;
use crate::http::{Http, Parent};
use crate::input::{
    AttendanceRequest, CollectRequest, JournalRequest, LoginRequest, MessagesRequest, NewsRequest,
    NotificationsRequest,
};
use crate::js;
use crate::login::{
    self, create_authenticated_http, has_configured_credentials, resolve_credentials,
    session_from_http,
};
use crate::session::{PARENT_URL, SavedSession, origin, session_path, trusted_url};
use crate::shapes::{self, Feed, MessagesPage};
use crate::signal::{Controller, Signal};
use crate::store::{self, Held, with_session};
use crate::upstream::Net;

const COLLECTION_TIMEOUT: Duration = Duration::from_secs(5 * 60);

const RENEWAL_TIMEOUT: Duration = Duration::from_secs(60);

const OVERVIEW_MAX_UNITS: usize = 40_000;

/// The news list endpoint when the communication app data does not name one.
const NEWS_LIST_PATH: &str = "Communication/News/GetNewsList";

/// A news body in the list is cut to this many UTF-16 units; infomentor_get_news_item returns it
/// up to `NEWS_ITEM_MAX_UNITS`.
const NEWS_LIST_MAX_UNITS: usize = 2_000;

const NEWS_ITEM_MAX_UNITS: usize = 40_000;

/// The page size used to find one item, since no single-item endpoint is known.
const NEWS_ITEM_PAGE_SIZE: i64 = 200;

/// The journal list endpoint when the documentation app data does not name one.
const JOURNAL_LIST_PATH: &str = "Documentation/Journal/GetJournalList";

/// Attendance app data. The public shell (`dist/scripts/scripts.js`) posts
/// `{baseUrl}{codeName}/{codeName}/appData` for every tile except preferences, and
/// `TILES.ATTENDANCE` is `attendance`. The same pattern is `documentation/documentation/appData`.
const ATTENDANCE_APP_DATA_PATH: &str = "attendance/attendance/appData";

/// A journal entry's text is cut to this many UTF-16 units.
const JOURNAL_MAX_UNITS: usize = 40_000;

/// Learning units (námslotur) and assessments (námsmat). Unverified signed-in: the paths below
/// come from the public shell, not from a signed-in response.
///
/// `https://minn.infomentor.is/dist/scripts/scripts.js` (Last-Modified: Mon, 27 Apr 2026
/// 09:47:52 GMT), fetched 2026-11-18. `TILES.UOL_V2` is `"uolv2"` and `TILES.ASSESSMENT_V2` is
/// `"assessmentv2"`. Opening a tile posts `{baseUrl}{codeName}/{codeName}/appData` (`loadView`,
/// `type:"POST"`), the same call as `attendance/attendance/appData`.
///
/// `#/uolv2/show/<id>` is the route `openUol` handles. It calls `curriedPostJSON(urls.getUol)`
/// with `{id}`. `postJSON` is `POST` `application/json; charset=utf-8` and `JSON.stringify` of
/// that object. The list on first open is the `uols` array of the app-data payload. `urls.getUols`
/// is only the academic-year refresh (`{academicYearId, stateFilter}`) and is not called here,
/// so a filter is not invented. `urls.persistFilter` and timeline posts are writes and are not
/// called.
///
/// `#/assessmentv2/show/<id>` selects that subject. The statement list is `fetchById(urls.getGridItems + "/")`
/// of `{gridId}?academicYearId={selected academicYearItems[].key}` (`fetchById` is `postJSON(url + suffix)`
/// with an undefined body). A subject with no grids uses `urls.getUolItems` the same way. Fields
/// shown by `dist/html/apps/assessmentv2/assessmentv2.statementItem.html`: `headerLabel`, `text`,
/// `markLastUpdated`, `markTextValue` / `markNumericValue`. `urls.updateNotificationState` marks
/// notifications read and is not called.
const LEARNING_APP_DATA: &str = "uolv2/uolv2/appData";

const ASSESSMENT_APP_DATA: &str = "assessmentv2/assessmentv2/appData";

/// Plain text cut to the same cap as a journal entry.
const STUDY_MAX_UNITS: usize = JOURNAL_MAX_UNITS;

/// `SessionOptions`.
#[derive(Default, Clone)]
pub struct Options {
    /// `--session`, resolved.
    pub session_file: Option<PathBuf>,
    /// `--credentials`, resolved.
    pub credentials_file: Option<PathBuf>,
    /// The store's keys; `None` uses the default provider.
    pub keys: Option<Arc<dyn KeyProvider>>,
}

/// What a read sees besides the HTTP session.
pub struct Context<'a> {
    pub signal: &'a Signal,
    /// `signal`, as the store's cancel flag.
    pub cancel: &'a Cancel,
    /// How the session is stored, for the status tool.
    pub storage: &'static str,
}

/// The HTTP session built from the saved one, and that saved session's JSON.
struct Active {
    http: Http,
    serialized: String,
}

/// `SetupStatus`, its fields in the order the TypeScript client sets them.
#[derive(Clone)]
struct SetupStatus {
    operation: Option<&'static str>,
    state: &'static str,
    message: String,
}

impl SetupStatus {
    fn to_json(&self) -> Value {
        match self.operation {
            Some(operation) => {
                json!({"operation": operation, "state": self.state, "message": self.message})
            }
            None => json!({"state": self.state, "message": self.message}),
        }
    }
}

/// A running login or import: what cancels it, and what reports its end.
struct Setup {
    controller: Controller,
    done: watch::Receiver<bool>,
}

pub struct Client {
    options: Options,
    net: Net,
    /// The queue: one operation at a time, in call order, holding the cached session.
    active: Arc<Mutex<Option<Active>>>,
    lifetime: Controller,
    closed: AtomicBool,
    /// The running login or import. It runs outside the queue; reads refuse while it runs.
    setup: std::sync::Mutex<Option<Setup>>,
    status: std::sync::Mutex<SetupStatus>,
    logging_out: AtomicBool,
}

/// `saveActive`: persist the session when a request changed it, never for another account.
fn save_active(
    active: &mut Active,
    previous: &SavedSession,
    held: &mut Held,
) -> Result<SavedSession> {
    let mut current = session_from_http(&active.http)?;

    if let (Some(previous), Some(current)) = (&previous.account_id, &current.account_id)
        && previous != current
    {
        return Err(Fail::new(
            Code::InvalidSession,
            "Refusing to replace the verified account during a school-data request. Sign in explicitly to change accounts.",
        ));
    }

    if current.account_id.is_none() {
        current.account_id.clone_from(&previous.account_id);
    }

    if current.selected_child_id.is_none() {
        current
            .selected_child_id
            .clone_from(&previous.selected_child_id);
    }

    if current.comparable() == previous.comparable() {
        active.serialized = previous.to_json().to_string();
        return Ok(previous.clone());
    }
    held.save(&current)?;
    active.serialized = current.to_json().to_string();
    Ok(current)
}

/// One read's signal and store cancel flag.
#[derive(Clone, Copy)]
struct Step<'a> {
    signal: &'a Signal,
    cancel: &'a Cancel,
}

impl Step<'_> {
    /// Prove the session on the parent page, which the read then reuses, save it, and read.
    fn verified<T>(
        self,
        active: &mut Active,
        saved: &mut SavedSession,
        held: &mut Held,
        read: &mut impl FnMut(&mut Http, &Context) -> Result<T>,
    ) -> Result<T> {
        let parent = active.http.read_parent(self.signal, None)?;

        if saved
            .account_id
            .as_ref()
            .is_some_and(|account| *account != parent.account_id)
        {
            return Err(Fail::new(
                Code::InvalidSession,
                "The saved session no longer matches its verified account. Sign in explicitly before continuing.",
            ));
        }
        *saved = save_active(active, saved, held)?;
        self.run(active, held, read)
    }

    fn run<T>(
        self,
        active: &mut Active,
        held: &Held,
        read: &mut impl FnMut(&mut Http, &Context) -> Result<T>,
    ) -> Result<T> {
        let context = Context {
            signal: self.signal,
            cancel: self.cancel,
            storage: held.storage,
        };
        let output = read(&mut active.http, &context)?;
        self.signal.check()?;
        Ok(output)
    }
}

fn timetable(http: &mut Http, signal: &Signal) -> Result<Feed> {
    http.read_app_data(
        "timetable/timetable/appData",
        &[],
        signal,
        shapes::timetable_response,
    )
}

fn messages_page(
    http: &mut Http,
    signal: &Signal,
    folder: &str,
    page: i64,
    page_size: i64,
    search: &str,
) -> Result<MessagesPage> {
    http.read_app_data(
        "Message/message/GetMessages",
        &[
            ("page", page.to_string()),
            ("pageSize", page_size.to_string()),
            ("messageText", search.to_owned()),
            ("inbox", (folder == "inbox").to_string()),
            ("sentItems", (folder == "sent").to_string()),
        ],
        signal,
        shapes::messages_response,
    )
}

fn message(http: &mut Http, signal: &Signal, id: i64) -> Result<Value> {
    http.read_app_data(
        "Message/message/GetMessage",
        &[("id", id.to_string())],
        signal,
        shapes::message_detail,
    )
}

fn notifications(http: &mut Http, signal: &Signal) -> Result<Feed> {
    http.read_app_data(
        "NotificationApp/NotificationApp/appData",
        &[],
        signal,
        shapes::notifications_response,
    )
}

/// A list URL from the app data, as a path under the parent site; `None` for anything off
/// minn.infomentor.is or carrying a query or fragment.
fn news_path(link: &str) -> Option<String> {
    let url = url::Url::parse(PARENT_URL).ok()?.join(link).ok()?;
    let url = trusted_url(url.as_str()).ok()?;
    let parent = url::Url::parse(PARENT_URL).ok()?;

    if origin(&url) != origin(&parent)
        || url.query().is_some_and(|query| !query.is_empty())
        || url.fragment().is_some_and(|fragment| !fragment.is_empty())
    {
        return None;
    }
    let path = url.path().trim_start_matches('/');
    (!path.is_empty()).then(|| path.to_owned())
}

/// The communication app's news access flag and list path. A failed or unexpected app-data read
/// falls back to the known endpoint, except for authentication, rate limits and cancellation,
/// which the caller must see.
fn news_source(http: &mut Http, signal: &Signal) -> Result<(Option<bool>, String)> {
    match http.read_app_data(
        "communication/communication/appData",
        &[],
        signal,
        shapes::communication_app_data,
    ) {
        Ok(data) => Ok((
            data["hasNewsAccess"].as_bool(),
            data["urls"]["getNewsList"]
                .as_str()
                .and_then(news_path)
                .unwrap_or_else(|| NEWS_LIST_PATH.to_owned()),
        )),
        Err(fail)
            if [
                Code::LoginRequired,
                Code::RateLimited,
                Code::Cancelled,
                Code::ChallengeRequired,
            ]
            .into_iter()
            .any(|code| fail.is(code)) =>
        {
            Err(fail)
        }
        Err(_) => Ok((None, NEWS_LIST_PATH.to_owned())),
    }
}

/// The news list of the selected child's school, and whether the account has news access.
fn news(http: &mut Http, signal: &Signal, page_size: Option<i64>) -> Result<(Option<bool>, Feed)> {
    let (access, path) = news_source(http, signal)?;

    if access == Some(false) {
        return Ok((
            access,
            Feed {
                items: Vec::new(),
                skipped: 0,
            },
        ));
    }
    let fields: Vec<(&str, String)> = page_size
        .map(|size| ("pageSize", size.to_string()))
        .into_iter()
        .collect();
    let feed = http.read_app_data(&path, &fields, signal, shapes::news_response)?;
    Ok((access, feed))
}

/// A parsed news item as the tools return it: the body as plain text, cut to `max` UTF-16 units,
/// and only attachments on an InfoMentor HTTPS host, with absolute URLs.
fn news_output(item: &Value, max: usize) -> Value {
    let content = html::plain_text(item["content"].as_str().unwrap_or_default());
    let mut omitted = 0;
    let attachments: Vec<Value> = item["attachments"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|attachment| {
            let url = attachment["url"]
                .as_str()
                .and_then(|link| url::Url::parse(PARENT_URL).ok()?.join(link).ok())
                .and_then(|url| trusted_url(url.as_str()).ok());

            match url {
                Some(url) => Some(json!({
                    "title": attachment["title"].as_str().unwrap_or_default(),
                    "url": url.as_str(),
                })),
                None => {
                    omitted += 1;
                    None
                }
            }
        })
        .collect();
    let mut output = json!({
        "id": item["id"],
        "title": js::trim(item["title"].as_str().unwrap_or_default()),
        "publishedDate": item["publishedDate"].as_str(),
        "publishedDateString": item["publishedDateString"].as_str(),
        "publishedBy": item["publishedBy"].as_str(),
        "content": js::slice_units(&content, max),
        "contentTruncated": js::units(&content) > max,
        "attachments": attachments,
    });

    if omitted > 0 {
        output["attachmentsOmitted"] = json!(omitted);
    }
    output
}

/// The selected child's attendance registrations, newest first as InfoMentor lists them.
fn attendance(http: &mut Http, signal: &Signal, input: AttendanceRequest) -> Result<Value> {
    let page = http.read_app_data(
        "attendance/attendance/GetAttendanceList",
        &[
            ("page", input.page.to_string()),
            ("pageSize", input.page_size.to_string()),
        ],
        signal,
        shapes::attendance_response,
    )?;
    let rows: Vec<Value> = page
        .items
        .iter()
        .map(|item| {
            let fields = shapes::ATTENDANCE_FIELDS.iter();
            Value::Object(
                fields
                    .map(|(key, _)| ((*key).to_owned(), item[*key].clone()))
                    .collect(),
            )
        })
        .collect();
    Ok(json!({
        "attendance": rows,
        "page": input.page,
        "pageSize": input.page_size,
        "totalItems": page.total,
        "more": page.more,
        "skipped": page.skipped,
        "retrievedAt": js::iso_string(js::now_ms()),
    }))
}

/// An attendance app-data field name: an ASCII identifier, so a label cannot carry a name.
fn attendance_field_name(key: &str) -> bool {
    let mut chars = key.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    first.is_ascii_alphabetic()
        && key.len() <= 64
        && chars.all(|character| character.is_ascii_alphanumeric())
}

/// A URL path from the attendance `urls` object. Query, fragment, user info and any host other
/// than `minn.infomentor.is` are dropped, so a token or cookie in the link is not returned.
fn attendance_action_path(link: &str) -> Option<String> {
    let mut url = url::Url::parse(PARENT_URL).ok()?.join(link).ok()?;
    url.set_query(None);
    url.set_fragment(None);
    let path = news_path(url.as_str())?;
    let characters = path
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'/' | b'-' | b'_' | b'.'));
    let mut segments = path.split('/').collect::<Vec<_>>();
    if segments.last() == Some(&"") {
        segments.pop();
    }
    let safe = characters
        && !segments.is_empty()
        && segments
            .iter()
            .all(|part| !part.is_empty() && *part != "." && *part != "..");
    safe.then(|| format!("/{path}"))
}

fn sorted_object(entries: impl Iterator<Item = (String, Value)>) -> Map<String, Value> {
    let mut entries: Vec<_> = entries.collect();
    entries.sort_by(|left, right| left.0.cmp(&right.0));
    entries.into_iter().collect()
}

/// Names and paths in `urls`, and top-level booleans. Every other field is omitted.
fn sanitize_attendance_app_data(value: &Value) -> Option<Value> {
    let object = value.as_object()?;
    let urls = object
        .get("urls")
        .and_then(Value::as_object)
        .into_iter()
        .flatten()
        .filter_map(|(key, link)| {
            let path = link.as_str().and_then(attendance_action_path)?;
            attendance_field_name(key).then(|| (key.clone(), Value::String(path)))
        });
    let flags = object.iter().filter_map(|(key, flag)| {
        if key == "urls" || !attendance_field_name(key) {
            return None;
        }
        Some((key.clone(), Value::Bool(flag.as_bool()?)))
    });
    Some(json!({
        "urls": Value::Object(sorted_object(urls)),
        "flags": Value::Object(sorted_object(flags)),
    }))
}

/// Permission flags and action paths for the attendance module. This posts only to
/// [`ATTENDANCE_APP_DATA_PATH`] and never to `registerAttendance` or any other write path.
fn read_attendance_app_data(http: &mut Http, signal: &Signal) -> Result<Value> {
    let view = http.read_app_data(
        ATTENDANCE_APP_DATA_PATH,
        &[],
        signal,
        sanitize_attendance_app_data,
    )?;
    Ok(json!({
        "urls": view["urls"],
        "flags": view["flags"],
        "retrievedAt": js::iso_string(js::now_ms()),
    }))
}

/// Whether the documentation app lists the journal tab, and its journal list path. A failed or
/// unexpected app-data read falls back to the known endpoint, except for authentication, rate
/// limits and cancellation, which the caller must see.
fn journal_source(http: &mut Http, signal: &Signal) -> Result<(Option<bool>, String)> {
    match http.read_app_data(
        "documentation/documentation/appData",
        &[],
        signal,
        shapes::documentation_app_data,
    ) {
        Ok(data) => Ok((
            data["tabs"]
                .as_array()
                .map(|tabs| tabs.iter().any(|tab| tab.as_str() == Some("journal"))),
            data["urls"]["getJournalList"]
                .as_str()
                .and_then(news_path)
                .unwrap_or_else(|| JOURNAL_LIST_PATH.to_owned()),
        )),
        Err(fail)
            if [
                Code::LoginRequired,
                Code::RateLimited,
                Code::Cancelled,
                Code::ChallengeRequired,
            ]
            .into_iter()
            .any(|code| fail.is(code)) =>
        {
            Err(fail)
        }
        Err(_) => Ok((None, JOURNAL_LIST_PATH.to_owned())),
    }
}

/// The journal entries of the selected child, and whether the account has journal access.
fn journal(
    http: &mut Http,
    signal: &Signal,
    page_size: Option<i64>,
) -> Result<(Option<bool>, Feed)> {
    let (access, path) = journal_source(http, signal)?;

    if access == Some(false) {
        return Ok((
            access,
            Feed {
                items: Vec::new(),
                skipped: 0,
            },
        ));
    }
    let fields: Vec<(&str, String)> = page_size
        .map(|size| ("pageSize", size.to_string()))
        .into_iter()
        .collect();
    let feed = http.read_app_data(&path, &fields, signal, shapes::journal_response)?;
    Ok((access, feed))
}

/// A parsed journal entry as the tool returns it: the body as plain text, cut to
/// `JOURNAL_MAX_UNITS` UTF-16 units, and only attachments on an InfoMentor HTTPS host, with
/// absolute URLs.
fn journal_output(item: &Value) -> Value {
    let text = html::plain_text(item["text"].as_str().unwrap_or_default());
    let mut omitted = 0;
    let attachments: Vec<Value> = item["attachments"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|attachment| {
            let url = attachment["url"]
                .as_str()
                .and_then(|link| url::Url::parse(PARENT_URL).ok()?.join(link).ok())
                .and_then(|url| trusted_url(url.as_str()).ok());

            match url {
                Some(url) => Some(json!({
                    "title": attachment["title"].as_str().unwrap_or_default(),
                    "url": url.as_str(),
                })),
                None => {
                    omitted += 1;
                    None
                }
            }
        })
        .collect();
    let categories: Vec<Value> = item["categories"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|category| category.as_str().map(|s| json!(js::trim(s))))
        .filter(|category| !category.as_str().unwrap_or_default().is_empty())
        .collect();
    let mut output = json!({
        "id": item["id"],
        "date": item["date"].as_str(),
        "time": item["time"].as_str(),
        "author": item["author"].as_str(),
        "categories": categories,
        "text": js::slice_units(&text, JOURNAL_MAX_UNITS),
        "textTruncated": js::units(&text) > JOURNAL_MAX_UNITS,
        "attachments": attachments,
    });

    if omitted > 0 {
        output["attachmentsOmitted"] = json!(omitted);
    }
    output
}

const UNEXPECTED_SCHOOL: Fail = Fail::new(
    Code::UnexpectedPage,
    "InfoMentor school data is unavailable or its format has changed.",
);

/// A 404 from one of these reads names what was missing. Any other failure is unchanged.
fn missing_page(fail: Fail, message: &'static str) -> Fail {
    match fail {
        Fail::Im {
            http_status: Some(404),
            ..
        } => fail.rewrap(message),
        other => other,
    }
}

fn school_object(http: &mut Http, signal: &Signal, path: &str) -> Result<Value> {
    http.read_app_data(path, &[], signal, |value| {
        value.as_object().is_some().then(|| value.clone())
    })
}

fn json_object(value: &Value) -> Option<Value> {
    value.as_object().is_some().then(|| value.clone())
}

/// A path from an app-data `urls` field, on this host, with no query or fragment.
fn study_path(data: &Value, key: &str) -> Option<String> {
    data.get("urls")?.get(key)?.as_str().and_then(news_path)
}

fn cap_study(text: String) -> (String, bool) {
    let truncated = js::units(&text) > STUDY_MAX_UNITS;
    (
        js::slice_units(&text, STUDY_MAX_UNITS).to_owned(),
        truncated,
    )
}

/// HTML as plain text. `None` when the field is present and not a string.
fn html_plain(value: Option<&Value>) -> Option<String> {
    match value {
        None | Some(Value::Null) => Some(String::new()),
        Some(Value::String(text)) => Some(html::plain_text(text)),
        Some(_) => None,
    }
}

fn array_of<'a>(value: &'a Value, key: &str) -> Option<Option<&'a Vec<Value>>> {
    match value.get(key) {
        None => Some(None),
        Some(Value::Array(items)) => Some(Some(items)),
        Some(_) => None,
    }
}

fn push_line(lines: &mut Vec<String>, line: String) {
    if !line.is_empty() {
        lines.push(line);
    }
}

fn labeled(label: String, value: String) -> String {
    match label.is_empty() {
        true => value,
        false => format!("{label}: {value}"),
    }
}

fn same_id(value: Option<&Value>, id: i64) -> bool {
    match value {
        Some(Value::Number(number)) => number.as_i64() == Some(id),
        Some(Value::String(text)) => text == &id.to_string(),
        _ => false,
    }
}

fn catalog_name(catalog: &[Value], id: i64) -> Option<String> {
    let name = js::trim(
        catalog
            .iter()
            .find(|subject| same_id(subject.get("id"), id))?
            .get("name")?
            .as_str()?,
    );
    (!name.is_empty()).then(|| name.to_owned())
}

/// One subject name. `Ok(None)` skips an id the catalog does not name.
fn one_subject(item: &Value, catalog: &[Value]) -> Option<Option<String>> {
    if let Some(text) = item.as_str() {
        let text = js::trim(text);
        return Some((!text.is_empty()).then(|| text.to_owned()));
    }

    if let Some(id) = item.as_i64() {
        return Some(catalog_name(catalog, id));
    }
    let object = item.as_object()?;

    if let Some(name) = object.get("name").and_then(Value::as_str) {
        let name = js::trim(name);

        if !name.is_empty() {
            return Some(Some(name.to_owned()));
        }
    }
    let id = object.get("id")?.as_i64()?;
    Some(catalog_name(catalog, id))
}

fn subject_names(value: &Value, catalog: &[Value]) -> Option<Vec<String>> {
    let mut names = Vec::new();

    for item in value.as_array()? {
        if let Some(name) = one_subject(item, catalog)? {
            names.push(name);
        }
    }
    Some(names)
}

fn catalog_of(data: &Value) -> &[Value] {
    data.get("subjects")
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or(&[])
}

/// An overview label that names the unit's period or dates. The label text is the school's.
fn is_period_label(label: &str) -> bool {
    let label = label.to_lowercase();
    ["period", "tímabil", "tidsperiod", "dagsetning", "datum"]
        .iter()
        .any(|needle| label.contains(needle))
        || label == "date"
        || label == "dates"
        || label.starts_with("date ")
        || label.starts_with("dates ")
}

fn dated_period(object: &Map<String, Value>) -> Option<String> {
    let text = |key: &str| {
        object
            .get(key)
            .and_then(Value::as_str)
            .map(js::trim)
            .filter(|text| !text.is_empty())
            .map(str::to_owned)
    };
    match (text("startDate"), text("endDate")) {
        (Some(start), Some(end)) => Some(format!("{start}–{end}")),
        (Some(start), None) => Some(start),
        (None, Some(end)) => Some(end),
        (None, None) => text("period"),
    }
}

fn push_overview(
    section: &Value,
    description: &mut Vec<String>,
    period: &mut Vec<String>,
) -> Option<()> {
    let Some(rows) = array_of(section, "overview")? else {
        return Some(());
    };
    let mut lines = Vec::new();

    for row in rows {
        let label = html_plain(row.get("label"))?;
        let value = html_plain(row.get("value"))?;

        if label.is_empty() && value.is_empty() {
            continue;
        }

        if is_period_label(&label) && !value.is_empty() {
            period.push(value.clone());
        }
        lines.push(labeled(label, value));
    }

    if !lines.is_empty() {
        description.push(lines.join("\n"));
    }
    Some(())
}

fn push_syllabus(section: &Value, goals: &mut Vec<String>) -> Option<()> {
    let Some(blocks) = array_of(section, "sections")? else {
        return Some(());
    };

    for block in blocks {
        let mut lines = Vec::new();
        push_line(&mut lines, html_plain(block.get("title"))?);
        let fields = array_of(block, "fields")?.map(Vec::as_slice).unwrap_or(&[]);

        for field in fields {
            let label = html_plain(field.get("label"))?;
            let value = html_plain(field.get("value"))?;

            if label.is_empty() && value.is_empty() {
                continue;
            }
            lines.push(labeled(label, value));
        }

        if !lines.is_empty() {
            goals.push(lines.join("\n"));
        }
    }
    Some(())
}

fn push_objectives(section: &Value, goals: &mut Vec<String>) -> Option<()> {
    let Some(items) = array_of(section, "objectives")? else {
        return Some(());
    };
    let mut lines = Vec::new();

    for item in items {
        push_line(&mut lines, html_plain(Some(item))?);
    }

    if !lines.is_empty() {
        goals.push(lines.join("\n"));
    }
    Some(())
}

fn statement_line(statement: &Value) -> Option<String> {
    let header = html_plain(statement.get("levelHeader"))?;
    let text = html_plain(statement.get("text"))?;
    Some(match (header.is_empty(), text.is_empty()) {
        (true, true) => String::new(),
        (false, true) => header,
        (true, false) => text,
        (false, false) => format!("{header}: {text}"),
    })
}

fn push_layers(statement: &Value, lines: &mut Vec<String>) -> Option<()> {
    let Some(layers) = array_of(statement, "layers")? else {
        return Some(());
    };

    for layer in layers {
        let title = html_plain(layer.get("title"))?;
        let text = html_plain(layer.get("freeText"))?;
        push_line(
            lines,
            match (title.is_empty(), text.is_empty()) {
                (true, true) => String::new(),
                (false, true) => title,
                (true, false) => text,
                (false, false) => format!("{title}: {text}"),
            },
        );
    }
    Some(())
}

fn push_statements(section: &Value, criteria: &mut Vec<String>) -> Option<()> {
    let Some(kinds) = array_of(section, "matrixTypes")? else {
        return Some(());
    };
    let mut lines = Vec::new();

    for kind in kinds {
        push_line(&mut lines, html_plain(kind.get("title"))?);
        let matrices = array_of(kind, "matrices")?
            .map(Vec::as_slice)
            .unwrap_or(&[]);

        for matrix in matrices {
            push_line(&mut lines, html_plain(matrix.get("title"))?);
            let statements = array_of(matrix, "statements")?
                .map(Vec::as_slice)
                .unwrap_or(&[]);

            for statement in statements {
                push_line(&mut lines, statement_line(statement)?);
                push_layers(statement, &mut lines)?;
                let grouped = array_of(statement, "groupedStatements")?
                    .map(Vec::as_slice)
                    .unwrap_or(&[]);

                for item in grouped {
                    push_line(&mut lines, statement_line(item)?);
                    push_layers(item, &mut lines)?;
                }
            }
        }
    }

    if !lines.is_empty() {
        criteria.push(lines.join("\n"));
    }
    Some(())
}

fn learning_list_item(value: &Value, catalog: &[Value]) -> Option<Value> {
    let id = value.get("id")?.as_i64()?;
    let title = js::trim(value.get("title")?.as_str()?).to_owned();

    if title.is_empty() {
        return None;
    }
    let subjects = match value.get("subjects") {
        None => Vec::new(),
        Some(subjects) => subject_names(subjects, catalog)?,
    };
    let description = match value.get("description") {
        None | Some(Value::Null) => Value::Null,
        Some(Value::String(text)) => Value::String(cap_study(html::plain_text(text)).0),
        Some(_) => return None,
    };
    Some(json!({
        "id": id,
        "title": title,
        "subjects": subjects,
        "description": description,
    }))
}

/// The unit the detail template shows: title, subjects, period, and the overview, syllabus,
/// objectives, and statements as plain text. Other section types (timeline, tasks, resources)
/// are not the requested text, and timeline posting is a write.
fn learning_unit_fields(id: i64, value: &Value, catalog: &[Value]) -> Option<Value> {
    let object = value.as_object()?;

    if object.contains_key("id") && !same_id(object.get("id"), id) {
        return None;
    }
    let title = js::trim(object.get("title")?.as_str()?).to_owned();

    if title.is_empty() {
        return None;
    }
    let subjects = match object.get("subjects") {
        None => Vec::new(),
        Some(subjects) => subject_names(subjects, catalog)?,
    };
    let sections = match object.get("sections") {
        None => &[][..],
        Some(Value::Array(items)) => items.as_slice(),
        Some(_) => return None,
    };
    let mut description = Vec::new();
    let mut goals = Vec::new();
    let mut criteria = Vec::new();
    let mut period = Vec::new();

    for section in sections {
        let heading = html_plain(section.get("title"))?;
        match section.get("type")?.as_str()? {
            "uol" => {
                push_line(&mut description, heading);
                push_overview(section, &mut description, &mut period)?;
            }
            "syllabus" => {
                push_line(&mut goals, heading);
                push_syllabus(section, &mut goals)?;
            }
            "objective" => {
                push_line(&mut goals, heading);
                push_objectives(section, &mut goals)?;
            }
            "statement" => {
                push_line(&mut criteria, heading);
                push_statements(section, &mut criteria)?;
            }
            _ => {}
        }
    }

    if period.is_empty() {
        if let Some(text) = dated_period(object) {
            period.push(text);
        }
    }
    let (description, description_cut) = cap_study(description.join("\n\n"));
    let (goals, goals_cut) = cap_study(goals.join("\n\n"));
    let (criteria, criteria_cut) = cap_study(criteria.join("\n\n"));
    let (period, period_cut) = match period.is_empty() {
        true => (Value::Null, false),
        false => {
            let (text, cut) = cap_study(period.join("\n"));
            (Value::String(text), cut)
        }
    };
    Some(json!({
        "id": id,
        "title": title,
        "subjects": subjects,
        "period": period,
        "description": description,
        "goals": goals,
        "criteria": criteria,
        "truncated": description_cut || goals_cut || criteria_cut || period_cut,
    }))
}

fn list_learning_units(http: &mut Http, signal: &Signal) -> Result<Value> {
    let data = school_object(http, signal, LEARNING_APP_DATA)
        .map_err(|fail| missing_page(fail, "InfoMentor did not return the learning units."))?;
    let units = data
        .get("uols")
        .and_then(Value::as_array)
        .ok_or(UNEXPECTED_SCHOOL)?;
    let catalog = catalog_of(&data);
    let mut shown = Vec::new();
    let mut skipped = 0;

    for unit in units {
        match learning_list_item(unit, catalog) {
            Some(unit) => shown.push(unit),
            None => skipped += 1,
        }
    }
    Ok(json!({
        "units": shown,
        "skipped": skipped,
        "retrievedAt": js::iso_string(js::now_ms()),
    }))
}

fn learning_unit(http: &mut Http, signal: &Signal, id: i64) -> Result<Value> {
    let data = school_object(http, signal, LEARNING_APP_DATA)
        .map_err(|fail| missing_page(fail, "InfoMentor did not return the learning units."))?;
    let path = study_path(&data, "getUol").ok_or(Fail::new(
        Code::UnexpectedPage,
        "InfoMentor did not name a learning-unit address.",
    ))?;
    let detail = http
        .read_json(&path, &json!({"id": id}).to_string(), signal, json_object)
        .map_err(|fail| missing_page(fail, "InfoMentor did not return this learning unit."))?;
    let unit = learning_unit_fields(id, &detail, catalog_of(&data)).ok_or(UNEXPECTED_SCHOOL)?;
    Ok(json!({
        "unit": unit,
        "retrievedAt": js::iso_string(js::now_ms()),
    }))
}

struct SubjectPlan {
    id: i64,
    title: String,
    grids: Vec<i64>,
    uols: Vec<i64>,
}

fn linked_ids(subject: &Value, key: &str) -> Option<Vec<i64>> {
    match array_of(subject, key)? {
        None => Some(Vec::new()),
        Some(items) => items
            .iter()
            .map(|item| item.as_i64().or_else(|| item.get("id")?.as_i64()))
            .collect(),
    }
}

fn subject_plan(subject: &Value) -> Option<SubjectPlan> {
    let title = js::trim(subject.get("title")?.as_str()?).to_owned();

    if title.is_empty() {
        return None;
    }
    Some(SubjectPlan {
        id: subject.get("id")?.as_i64()?,
        title,
        grids: linked_ids(subject, "linkedGrids")?,
        uols: linked_ids(subject, "linkedUols")?,
    })
}

fn selected_year(data: &Value) -> Option<String> {
    let selected = data
        .get("academicYearItems")?
        .as_array()?
        .iter()
        .find(|item| item.get("selected").and_then(Value::as_bool) == Some(true))?;
    match selected.get("key")? {
        Value::String(text) if !text.is_empty() => Some(text.clone()),
        Value::Number(number) => number.as_i64().map(|number| number.to_string()),
        _ => None,
    }
}

fn assessment_result(item: &Value) -> Option<Option<String>> {
    if let Some(text) = item.get("markTextValue") {
        match text {
            Value::Null => {}
            Value::String(text) => {
                let text = js::trim(text);

                if !text.is_empty() {
                    return Some(Some(text.to_owned()));
                }
            }
            _ => return None,
        }
    }
    Some(match item.get("markNumericValue") {
        None | Some(Value::Null) => None,
        Some(Value::String(text)) => {
            let text = js::trim(text);
            (!text.is_empty()).then(|| text.to_owned())
        }
        Some(Value::Number(number)) => Some(number.to_string()),
        Some(_) => return None,
    })
}

fn statement_row(subject: &str, item: &Value) -> Option<Value> {
    let id = item.get("id")?.as_i64()?;
    let title = html_plain(item.get("headerLabel"))?;
    let (text, truncated) = cap_study(html_plain(item.get("text"))?);
    let date = match item.get("markLastUpdated") {
        None | Some(Value::Null) => Value::Null,
        Some(Value::String(text)) => Value::String(text.clone()),
        Some(_) => return None,
    };
    Some(json!({
        "id": id,
        "subject": subject,
        "title": title,
        "date": date,
        "result": assessment_result(item)?,
        "text": text,
        "textTruncated": truncated,
    }))
}

fn subject_row(plan: &SubjectPlan) -> Value {
    json!({
        "id": plan.id,
        "subject": plan.title,
        "title": plan.title,
        "date": Value::Null,
        "result": Value::Null,
        "text": "",
        "textTruncated": false,
    })
}

fn statement_page(
    http: &mut Http,
    signal: &Signal,
    path: &str,
    id: i64,
    year: &str,
    subject: &str,
) -> Result<(Vec<Value>, usize)> {
    let target = format!(
        "{}/{id}?academicYearId={}",
        path.trim_end_matches('/'),
        js::encode_query(year)
    );
    let page = http
        .read_json(&target, "", signal, json_object)
        .map_err(|fail| missing_page(fail, "InfoMentor did not return this assessment."))?;
    let items = page
        .get("statementItemsList")
        .and_then(Value::as_array)
        .ok_or(UNEXPECTED_SCHOOL)?;
    let mut rows = Vec::new();
    let mut skipped = 0;

    for item in items {
        match statement_row(subject, item) {
            Some(row) => rows.push(row),
            None => skipped += 1,
        }
    }
    Ok((rows, skipped))
}

fn assessments(http: &mut Http, signal: &Signal, id: Option<i64>) -> Result<Value> {
    let data = school_object(http, signal, ASSESSMENT_APP_DATA)
        .map_err(|fail| missing_page(fail, "InfoMentor did not return the assessments."))?;
    let subjects = data
        .get("navigationModel")
        .and_then(|model| model.get("navigationItemsList"))
        .and_then(Value::as_array)
        .ok_or(UNEXPECTED_SCHOOL)?;
    let mut plans = Vec::new();
    let mut skipped = 0;

    for subject in subjects {
        match subject_plan(subject) {
            Some(plan) => plans.push(plan),
            None => skipped += 1,
        }
    }

    if let Some(id) = id
        && !plans.iter().any(|plan| plan.id == id)
    {
        return Err(Fail::config(
            "This assessment is not in the current list. Use an id from infomentor_get_assessments.",
        ));
    }
    let mut rows = Vec::new();

    for plan in plans
        .iter()
        .filter(|plan| id.is_none_or(|id| plan.id == id))
    {
        let (kind, ids) = match plan.grids.is_empty() {
            false => ("getGridItems", plan.grids.as_slice()),
            true => ("getUolItems", plan.uols.as_slice()),
        };

        if ids.is_empty() {
            rows.push(subject_row(plan));
            continue;
        }
        let path = study_path(&data, kind).ok_or(Fail::new(
            Code::UnexpectedPage,
            "InfoMentor did not name an assessment address.",
        ))?;
        let year = selected_year(&data).ok_or(Fail::new(
            Code::UnexpectedPage,
            "InfoMentor did not name the assessment year.",
        ))?;
        let mut found = Vec::new();

        for item in ids {
            let (page, page_skipped) =
                statement_page(http, signal, &path, *item, &year, &plan.title)?;
            skipped += page_skipped;
            found.extend(page);
        }

        if found.is_empty() {
            rows.push(subject_row(plan));
        } else {
            rows.extend(found);
        }
    }
    Ok(json!({
        "assessments": rows,
        "skipped": skipped,
        "retrievedAt": js::iso_string(js::now_ms()),
    }))
}

fn text<'v>(value: &'v Value, key: &str) -> &'v str {
    value[key].as_str().unwrap_or_default()
}

/// The overview of the selected child, after selecting `child_id` when given.
fn overview(http: &mut Http, signal: &Signal, child_id: Option<&str>) -> Result<Value> {
    let mut read = || -> Result<Value> {
        let parent = match (child_id, &http.parent) {
            (None, Some(parent)) => parent.clone(),
            _ => http.read_parent(signal, child_id)?,
        };
        let (timetable, skipped) = match parent.has_timetable() {
            true => {
                let feed = timetable(http, signal)?;
                (Some(feed.items), feed.skipped)
            }
            false => (None, 0),
        };
        let mut lines: Vec<String> = parent
            .pupils
            .iter()
            .map(|pupil| {
                format!(
                    "{}{}",
                    pupil.name,
                    if pupil.selected { " (selected)" } else { "" }
                )
            })
            .collect();

        for item in timetable.iter().flatten() {
            lines.push(format!(
                "{}: {} ({}–{})",
                text(item, "start"),
                text(item, "title"),
                text(item, "startTime"),
                text(item, "endTime")
            ));
        }
        let text = lines.join("\n");
        Ok(json!({
            "title": "InfoMentor parent overview",
            "text": js::slice_units(&text, OVERVIEW_MAX_UNITS),
            "truncated": js::units(&text) > OVERVIEW_MAX_UNITS,
            "children": parent
                .pupils
                .iter()
                .map(|pupil| json!({"id": pupil.id, "name": pupil.name, "selected": pupil.selected}))
                .collect::<Vec<_>>(),
            "timetable": timetable,
            "skipped": skipped,
            "retrievedAt": js::iso_string(js::now_ms()),
        }))
    };

    match (read(), child_id) {
        (Err(fail), Some(_)) => Err(fail.rewrap(
            "InfoMentor could not load the selected child. Selection may have changed; refresh infomentor_get_overview before continuing.",
        )),
        (outcome, _) => outcome,
    }
}

/// A collection's reads through the session; the parent page the read already verified is used
/// once, as the first parent read.
struct Reads<'h> {
    http: &'h mut Http,
    cached: Option<Parent>,
}

impl Source for Reads<'_> {
    fn get_parent(&mut self, signal: &Signal) -> Result<Parent> {
        match self.cached.take() {
            Some(parent) => Ok(parent),
            None => self.http.read_parent(signal, None),
        }
    }

    fn select_child(&mut self, child_id: &str, signal: &Signal) -> Result<Parent> {
        self.http.read_parent(signal, Some(child_id))
    }

    fn read_timetable(&mut self, parent: &Parent, signal: &Signal) -> Result<Option<Feed>> {
        match parent.has_timetable() {
            true => timetable(self.http, signal).map(Some),
            false => Ok(None),
        }
    }

    fn get_messages(&mut self, folder: &str, page: i64, signal: &Signal) -> Result<MessagesPage> {
        messages_page(self.http, signal, folder, page, 100, "")
    }

    fn get_message(&mut self, id: i64, signal: &Signal) -> Result<Value> {
        message(self.http, signal, id)
    }

    fn get_notifications(&mut self, signal: &Signal) -> Result<Feed> {
        notifications(self.http, signal)
    }
}

impl Client {
    /// `None` outside a Tokio runtime or when no HTTP client can be built.
    pub fn new(options: Options) -> Option<Self> {
        Some(Self {
            options,
            net: Net::new()?,
            active: Arc::default(),
            lifetime: Controller::default(),
            closed: AtomicBool::new(false),
            setup: std::sync::Mutex::default(),
            status: std::sync::Mutex::new(SetupStatus {
                operation: None,
                state: "idle",
                message: "No setup operation has started.".to_owned(),
            }),
            logging_out: AtomicBool::new(false),
        })
    }

    /// Queue `read` behind earlier operations and run it on a blocking thread with the session
    /// held: verified on the parent page first, and saved again afterwards when it changed.
    async fn read<T: Send + 'static>(
        self: &Arc<Self>,
        signal: Signal,
        read: impl FnMut(&mut Http, &Context) -> Result<T> + Send + 'static,
    ) -> Result<T> {
        let signal = signal.any(&self.lifetime.signal());
        // A local queue preserves call order; the store lock also excludes other MCP processes.
        let mut active = self.active.clone().lock_owned().await;
        let client = self.clone();

        tokio::task::spawn_blocking(move || client.held_read(&mut active, &signal, read))
            .await
            .unwrap_or(Err(Fail::Unknown))
    }

    fn held_read<T>(
        &self,
        active: &mut Option<Active>,
        signal: &Signal,
        mut read: impl FnMut(&mut Http, &Context) -> Result<T>,
    ) -> Result<T> {
        signal.check()?;

        if self.closed.load(Ordering::SeqCst) {
            return Err(Fail::new(
                Code::Cancelled,
                "This InfoMentor client has been closed.",
            ));
        }

        if self.setting_up() || self.logging_out.load(Ordering::SeqCst) {
            return Err(Fail::new(
                Code::OperationInProgress,
                "Account setup is in progress. Check infomentor_setup_status before reading school data.",
            ));
        }
        let file = session_path(self.options.session_file.as_deref())?;
        let bridge = signal.store_cancel(&self.net.handle);

        with_session(&file, &bridge.cancel, self.options.keys.clone(), |held| {
            let mut saved = held.session.clone();
            let serialized = saved.to_json().to_string();

            if active
                .as_ref()
                .is_none_or(|active| active.serialized != serialized)
            {
                *active = None;
            }
            let current = active.get_or_insert_with(|| Active {
                http: Http::new(saved.jar(), saved.cooldown(), self.net.clone()),
                serialized,
            });
            let step = Step {
                signal,
                cancel: &bridge.cancel,
            };

            match step.verified(current, &mut saved, held, &mut read) {
                Err(fail) if fail.is(Code::LoginRequired) => {
                    self.renew(active, saved, held, step, &mut read, fail)
                }
                outcome => {
                    if !signal.aborted() {
                        save_active(current, &saved, held)?;
                    }
                    outcome
                }
            }
        })
    }

    /// The renewal after a confirmed authentication expiry: one submission of the stored sign-in,
    /// or else the configured one, for the same account, then the read replayed once. Until the
    /// renewed session is saved, nothing replaces the expired one, so a failure keeps it.
    fn renew<T>(
        &self,
        active: &mut Option<Active>,
        mut saved: SavedSession,
        held: &mut Held,
        step: Step,
        read: &mut impl FnMut(&mut Http, &Context) -> Result<T>,
        expired: Fail,
    ) -> Result<T> {
        if held.credentials.is_none() && !has_configured_credentials(&self.options) {
            return Err(expired);
        }
        let verified = active
            .as_ref()
            .and_then(|active| active.http.parent.as_ref())
            .map(|parent| parent.account_id.clone());
        let Some(account_id) = saved.account_id.clone().or(verified) else {
            return Err(Fail::new(
                Code::LoginRequired,
                "This older session expired before its account could be verified. Call infomentor_login once to enable automatic authentication refresh.",
            ));
        };
        // The stored sign-in comes first; one submission, never a second source after a rejection.
        let (credentials, _) =
            resolve_credentials(&self.options, step.signal, held.credentials.as_ref())?;
        let mut candidate = create_authenticated_http(
            &self.net,
            step.signal,
            credentials,
            &Signal::timeout(RENEWAL_TIMEOUT),
        )?;

        if candidate.parent.as_ref().map(|parent| &parent.account_id) != Some(&account_id) {
            return Err(Fail::new(
                Code::LoginRequired,
                "The stored or configured credentials belong to a different InfoMentor account. The previous session was kept. Correct the private credentials or explicitly sign in to change accounts.",
            ));
        }

        if let Some(child) = &saved.selected_child_id {
            candidate.read_parent(step.signal, Some(child))?;
        }
        let mut renewed = Active {
            http: candidate,
            serialized: String::new(),
        };
        saved = save_active(&mut renewed, &saved, held)?;
        let current = active.insert(renewed);
        // Only confirmed authentication expiry replays a read, once. Other failures propagate.
        let outcome = step.run(current, held, read);

        if !step.signal.aborted() {
            save_active(current, &saved, held)?;
        }
        outcome
    }

    pub async fn overview(self: &Arc<Self>, signal: Signal) -> Result<Value> {
        self.read(signal, |http, context| overview(http, context.signal, None))
            .await
    }

    pub async fn select_child(self: &Arc<Self>, child_id: String, signal: Signal) -> Result<Value> {
        self.read(signal, move |http, context| {
            overview(http, context.signal, Some(&child_id))
        })
        .await
    }

    pub async fn messages(
        self: &Arc<Self>,
        input: MessagesRequest,
        signal: Signal,
    ) -> Result<Value> {
        self.read(signal, move |http, context| {
            let page = messages_page(
                http,
                context.signal,
                input.folder,
                input.page,
                input.page_size,
                &input.search,
            )?;
            // The live endpoint reports page: 0 even when it correctly applies a requested page.
            Ok(json!({
                "items": page.items,
                "skipped": page.skipped,
                "more": page.more,
                "page": input.page,
                "pageSize": input.page_size,
                "folder": input.folder,
                "retrievedAt": js::iso_string(js::now_ms()),
            }))
        })
        .await
    }

    pub async fn message(self: &Arc<Self>, id: i64, signal: Signal) -> Result<Value> {
        self.read(signal, move |http, context| {
            Ok(json!({
                "message": message(http, context.signal, id)?,
                "retrievedAt": js::iso_string(js::now_ms()),
            }))
        })
        .await
    }

    pub async fn notifications(
        self: &Arc<Self>,
        input: NotificationsRequest,
        signal: Signal,
    ) -> Result<Value> {
        self.read(signal, move |http, context| {
            let feed = notifications(http, context.signal)?;
            let shown: Vec<Value> = feed
                .items
                .into_iter()
                .filter(|item| {
                    (input.include_cleared || item["state"] != "Cleared")
                        && (!input.selected_child_only
                            || item["currentlySelectedPupil"] == Value::Bool(true))
                })
                .collect();
            Ok(json!({
                "notifications": shown,
                "skipped": feed.skipped,
                "selectedChildOnly": input.selected_child_only,
                "includeCleared": input.include_cleared,
                "retrievedAt": js::iso_string(js::now_ms()),
            }))
        })
        .await
    }

    /// infomentor_get_news: the newest `limit` news items of the selected child's school.
    pub async fn news(self: &Arc<Self>, input: NewsRequest, signal: Signal) -> Result<Value> {
        self.read(signal, move |http, context| {
            let (access, feed) = news(http, context.signal, input.page_size)?;
            let total = feed.items.len();
            let shown: Vec<Value> = feed
                .items
                .iter()
                .take(usize::try_from(input.limit).unwrap_or(usize::MAX))
                .map(|item| news_output(item, NEWS_LIST_MAX_UNITS))
                .collect();
            Ok(json!({
                "news": shown,
                "total": total,
                "skipped": feed.skipped,
                "hasNewsAccess": access,
                "retrievedAt": js::iso_string(js::now_ms()),
            }))
        })
        .await
    }

    /// infomentor_get_news_item: one item of the news list by id, its body in full (to a cap).
    pub async fn news_item(self: &Arc<Self>, id: i64, signal: Signal) -> Result<Value> {
        self.read(signal, move |http, context| {
            let (_, feed) = news(http, context.signal, Some(NEWS_ITEM_PAGE_SIZE))?;
            let item = feed
                .items
                .iter()
                .find(|item| item["id"].as_i64() == Some(id))
                .ok_or(Fail::config(
                    "This news item is not in the current news list. Use an id from infomentor_get_news.",
                ))?;
            Ok(json!({
                "news": news_output(item, NEWS_ITEM_MAX_UNITS),
                "retrievedAt": js::iso_string(js::now_ms()),
            }))
        })
        .await
    }

    /// infomentor_get_attendance: one page of the selected child's attendance registrations.
    pub async fn attendance(
        self: &Arc<Self>,
        input: AttendanceRequest,
        signal: Signal,
    ) -> Result<Value> {
        self.read(signal, move |http, context| {
            attendance(http, context.signal, input)
        })
        .await
    }

    /// infomentor_get_attendance_app_data: action paths and top-level boolean flags. Only reads.
    pub async fn attendance_app_data(self: &Arc<Self>, signal: Signal) -> Result<Value> {
        self.read(signal, move |http, context| {
            read_attendance_app_data(http, context.signal)
        })
        .await
    }

    /// infomentor_list_learning_units: the learning units the app data lists for the selected child.
    pub async fn learning_units(self: &Arc<Self>, signal: Signal) -> Result<Value> {
        self.read(signal, move |http, context| {
            list_learning_units(http, context.signal)
        })
        .await
    }

    /// infomentor_get_learning_unit: one learning unit, its text as plain text.
    pub async fn learning_unit(self: &Arc<Self>, id: i64, signal: Signal) -> Result<Value> {
        self.read(signal, move |http, context| {
            learning_unit(http, context.signal, id)
        })
        .await
    }

    /// infomentor_get_assessments: assessment statements for every subject, or one subject.
    pub async fn assessments(self: &Arc<Self>, id: Option<i64>, signal: Signal) -> Result<Value> {
        self.read(signal, move |http, context| {
            assessments(http, context.signal, id)
        })
        .await
    }

    /// infomentor_get_journal: the newest `limit` journal entries of the selected child.
    pub async fn journal(self: &Arc<Self>, input: JournalRequest, signal: Signal) -> Result<Value> {
        self.read(signal, move |http, context| {
            let (access, feed) = journal(http, context.signal, input.page_size)?;
            let total = feed.items.len();
            let shown: Vec<Value> = feed
                .items
                .iter()
                .take(usize::try_from(input.limit).unwrap_or(usize::MAX))
                .map(journal_output)
                .collect();
            Ok(json!({
                "entries": shown,
                "total": total,
                "skipped": feed.skipped,
                "hasJournalAccess": access,
                "retrievedAt": js::iso_string(js::now_ms()),
            }))
        })
        .await
    }

    pub async fn collect(self: &Arc<Self>, input: CollectRequest, signal: Signal) -> Result<Value> {
        let signal = signal.any(&Signal::timeout(COLLECTION_TIMEOUT));
        let session_file = self.options.session_file.clone();

        self.read(signal, move |http, context| {
            let file = session_path(session_file.as_deref())?;
            let cached = http.parent.clone();
            let mut reads = Reads { http, cached };
            collect_updates(&input, &file, context.signal, context.cancel, &mut reads)
        })
        .await
    }

    /// `getSessionStatus`: an expired or missing session is a status, not a failure.
    pub async fn session_status(self: &Arc<Self>, signal: Signal) -> Result<Value> {
        let status = self
            .read(signal, |_, context| {
                Ok(json!({"authenticated": true, "storage": context.storage}))
            })
            .await;

        match status {
            Err(
                fail @ Fail::Im {
                    code: Code::LoginRequired,
                    ..
                },
            ) => Ok(json!({
                "authenticated": false,
                "nextStep": mcp_runtime::Failure::safe_message(fail),
            })),
            other => other,
        }
    }

    fn setting_up(&self) -> bool {
        self.setup
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .is_some()
    }

    fn set_status(&self, status: SetupStatus) {
        *self.status.lock().unwrap_or_else(PoisonError::into_inner) = status;
    }

    /// `getSetupStatus`.
    pub fn setup_status(&self) -> Value {
        self.status
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .to_json()
    }

    /// `startLogin`: start a login or import and return at once; its result becomes the setup
    /// status.
    pub fn start_login(self: &Arc<Self>, request: LoginRequest) -> Result<Value> {
        if request.import_file.is_some() && request.credentials_file.is_some() {
            return Err(Fail::config("Choose session import or login, not both."));
        }

        if self.closed.load(Ordering::SeqCst) {
            return Err(Fail::new(
                Code::Cancelled,
                "This InfoMentor client has been closed.",
            ));
        }
        let mut setup = self.setup.lock().unwrap_or_else(PoisonError::into_inner);

        if setup.is_some() || self.logging_out.load(Ordering::SeqCst) {
            return Err(Fail::new(
                Code::OperationInProgress,
                "Another setup operation is active. Check its status or cancel it first.",
            ));
        }
        let operation = match request.import_file {
            Some(_) => "import",
            None => "login",
        };
        self.set_status(SetupStatus {
            operation: Some(operation),
            state: "running",
            message: "Setup started. Check infomentor_setup_status for progress.".to_owned(),
        });
        let controller = Controller::default();
        let signal = controller.signal();
        let (finished, done) = watch::channel(false);
        let client = self.clone();

        tokio::spawn(async move {
            let outcome = client.run_setup(request, &signal).await;
            let (state, message) = match outcome {
                Ok(advice) => (
                    "succeeded",
                    std::iter::once(
                        "Session saved in the encrypted store. Call infomentor_session_status to verify access."
                            .to_owned(),
                    )
                    .chain(advice.map(|file| store::delete_credentials_advice(&file)))
                    .collect::<Vec<_>>()
                    .join(" "),
                ),
                Err(_) if signal.aborted() => (
                    "cancelled",
                    "Setup cancelled. The previously saved session was kept.".to_owned(),
                ),
                Err(fail @ Fail::Im { .. }) => ("failed", mcp_runtime::Failure::safe_message(fail).unwrap().into_owned()),
                Err(_) => (
                    "failed",
                    "Setup failed. Check the network and session-file permissions.".to_owned(),
                ),
            };
            client.set_status(SetupStatus {
                operation: Some(operation),
                state,
                message,
            });
            *client.setup.lock().unwrap_or_else(PoisonError::into_inner) = None;
            finished.send_replace(true);
        });
        *setup = Some(Setup { controller, done });
        drop(setup);
        Ok(self.setup_status())
    }

    /// The setup itself, after the operations queued before it. Returns the credentials file to
    /// advise deleting: only one this call named, never the server's configured path.
    async fn run_setup(
        self: &Arc<Self>,
        request: LoginRequest,
        signal: &Signal,
    ) -> Result<Option<String>> {
        {
            let mut active = self.active.lock().await;
            signal.check()?;
            *active = None;
        }
        let client = self.clone();
        let signal = signal.clone();

        tokio::task::spawn_blocking(move || {
            let allow = request.allow_account_change.unwrap_or(false);

            if let Some(file) = &request.import_file {
                login::import_session(
                    &client.net,
                    &client.options,
                    Path::new(file),
                    &signal,
                    allow,
                )?;
                return Ok(None);
            }
            let named = request.credentials_file.is_some();
            let options = Options {
                credentials_file: request
                    .credentials_file
                    .map(PathBuf::from)
                    .or_else(|| client.options.credentials_file.clone()),
                ..client.options.clone()
            };
            let file = login::login(
                &client.net,
                &options,
                &signal,
                allow,
                request.timeout_seconds as f64 * 1000.0,
            )?;
            Ok(file.filter(|_| named))
        })
        .await
        .unwrap_or(Err(Fail::Unknown))
    }

    /// `cancelSetup`: abort the running setup, wait for it to end, and report the status.
    pub async fn cancel_setup(&self) -> Value {
        let done = self
            .setup
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .as_ref()
            .map(|setup| {
                setup.controller.abort();
                setup.done.clone()
            });

        if let Some(mut done) = done {
            // A dropped sender means the task ended without reporting; it has ended either way.
            let _ = done.wait_for(|finished| *finished).await;
        }
        self.setup_status()
    }

    /// `logout`: cancel any setup, wait for queued operations, then remove the local session, the
    /// stored sign-in and the collection cursors.
    pub async fn logout(&self) -> Result<()> {
        if self.logging_out.swap(true, Ordering::SeqCst) {
            return Err(Fail::new(
                Code::OperationInProgress,
                "Logout is already in progress.",
            ));
        }
        let outcome = async {
            self.cancel_setup().await;
            *self.active.lock().await = None;
            let file = session_path(self.options.session_file.as_deref())?;
            let keys = self.options.keys.clone();
            tokio::task::spawn_blocking(move || store::logout(&file, keys))
                .await
                .unwrap_or(Err(Fail::Unknown))?;
            self.set_status(SetupStatus {
                operation: None,
                state: "idle",
                message: "Local session and stored sign-in removed. Call infomentor_login to sign in again."
                    .to_owned(),
            });
            Ok(())
        }
        .await;
        self.logging_out.store(false, Ordering::SeqCst);
        outcome
    }

    /// Stop every operation: queued and running reads, and a running setup, end with a
    /// cancellation.
    pub fn abort(&self) {
        self.closed.store(true, Ordering::SeqCst);
        self.lifetime.abort();

        if let Some(setup) = self
            .setup
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .as_ref()
        {
            setup.controller.abort();
        }
    }

    /// `close`: abort, then wait for the setup and the running operation to end.
    pub async fn close(&self) {
        self.abort();
        self.cancel_setup().await;
        *self.active.lock().await = None;
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{
        learning_list_item, learning_unit_fields, sanitize_attendance_app_data, statement_row,
    };

    #[test]
    fn attendance_app_data_keeps_paths_and_top_level_booleans_only() {
        let sanitized = sanitize_attendance_app_data(&json!({
            "urls": {
                "registerAttendance": "/attendance/attendance/registerAttendance/",
                "saveAbsenceComment": "/attendance/attendance/saveAbsenceComment?access_token=synthetic-token",
                "createLeaveRequest": "https://minn.infomentor.is/attendance/attendance/createLeaveRequest",
                "deleteLeaveRequest": "https://evil.example/deleteLeaveRequest",
                "getLeaveRequestList": "/attendance/attendance/getLeaveRequestList#synthetic-token",
                "withUserInfo": "https://user:pass@minn.infomentor.is/attendance/attendance/registerAttendance",
                "not a key": "/attendance/attendance/skipped",
                "pupilName": "Example Student",
                "script": "javascript:alert(1)",
            },
            "canRegisterDayAbsence": true,
            "canRegisterSessionAbsence": false,
            "absenceTodayLocked": true,
            "absenceTomorrowLocked": false,
            "canAddSicknessToday": true,
            "canAddSicknessTomorrow": false,
            "autoConfirmAbsence": true,
            "isParent": true,
            "not a flag": true,
            "minutes": 4,
            "displayName": "Example Student",
            "absenceTodayComment": "example comment",
            "sessionCookie": "IMHome=synthetic-cookie",
            "token": "synthetic-token",
            "nested": {"canRegisterDayAbsence": false, "displayName": "Example Student"},
        }))
        .unwrap();
        assert_eq!(
            sanitized,
            json!({
                "urls": {
                    "createLeaveRequest": "/attendance/attendance/createLeaveRequest",
                    "getLeaveRequestList": "/attendance/attendance/getLeaveRequestList",
                    "registerAttendance": "/attendance/attendance/registerAttendance/",
                    "saveAbsenceComment": "/attendance/attendance/saveAbsenceComment",
                },
                "flags": {
                    "absenceTodayLocked": true,
                    "absenceTomorrowLocked": false,
                    "autoConfirmAbsence": true,
                    "canAddSicknessToday": true,
                    "canAddSicknessTomorrow": false,
                    "canRegisterDayAbsence": true,
                    "canRegisterSessionAbsence": false,
                    "isParent": true,
                },
            })
        );
        let text = sanitized.to_string();
        assert!(!text.contains("synthetic-token"));
        assert!(!text.contains("synthetic-cookie"));
        assert!(!text.contains("Example Student"));
        assert!(!text.contains("example comment"));
        assert!(!text.contains("evil.example"));
        assert!(sanitize_attendance_app_data(&json!([])).is_none());
        assert!(sanitize_attendance_app_data(&json!("attendance")).is_none());
    }

    #[test]
    fn learning_unit_text_is_plain_and_drops_timeline() {
        let catalog = json!([{"id": 4, "name": "Synthetic subject"}]);
        let unit = learning_unit_fields(
            1_000_001,
            &json!({
                "id": 1000001,
                "title": " Synthetic unit ",
                "subjects": [4, {"name": "Named"}],
                "sections": [
                    {"type": "uol", "title": "About", "overview": [
                        {"label": "Lýsing", "value": "<p>Learn <b>this</b>.</p>"},
                        {"label": "Tímabil", "value": "1.1.2026–1.6.2026"}
                    ]},
                    {"type": "syllabus", "sections": [
                        {"title": "Goals", "fields": [{"label": "Aim", "value": "<p>Read</p>"}]}
                    ]},
                    {"type": "objective", "objectives": ["<p>Count</p>"]},
                    {"type": "statement", "matrixTypes": [{
                        "title": "Criteria",
                        "matrices": [{"title": "Level", "statements": [{
                            "text": "<p>Explain</p>",
                            "levelHeader": "A",
                            "groupedStatements": [],
                            "layers": [{"title": "Layer", "freeText": "<p>More</p>"}]
                        }]}]
                    }]},
                    {"type": "timeline", "entries": [{"text": "not included"}]}
                ]
            }),
            catalog.as_array().unwrap(),
        )
        .unwrap();
        assert_eq!(
            unit,
            json!({
                "id": 1000001,
                "title": "Synthetic unit",
                "subjects": ["Synthetic subject", "Named"],
                "period": "1.1.2026–1.6.2026",
                "description": "About\n\nLýsing: Learn this.\nTímabil: 1.1.2026–1.6.2026",
                "goals": "Goals\nAim: Read\n\nCount",
                "criteria": "Criteria\nLevel\nA: Explain\nLayer: More",
                "truncated": false,
            })
        );
        assert!(learning_unit_fields(1, &json!({"title": "x", "id": 2}), &[]).is_none());
        assert!(learning_unit_fields(1, &json!({"id": 1, "sections": {}}), &[]).is_none());
        let listed = learning_list_item(
            &json!({"id": 7, "title": "Unit", "subjects": [4], "description": "<p>Short</p>"}),
            catalog.as_array().unwrap(),
        )
        .unwrap();
        assert_eq!(listed["description"], "Short");
        assert_eq!(listed["subjects"], json!(["Synthetic subject"]));
        assert!(learning_list_item(&json!({"id": "no", "title": "x"}), &[]).is_none());
    }

    #[test]
    fn assessment_rows_keep_the_mark_and_drop_a_bad_statement() {
        let row = statement_row(
            "Synthetic subject",
            &json!({
                "id": 8,
                "headerLabel": "Synthetic mark",
                "text": "<p>Did <b>well</b>.</p>",
                "markLastUpdated": "01.02.2026",
                "markTextValue": "A",
                "markNumericValue": 4
            }),
        )
        .unwrap();
        assert_eq!(
            row,
            json!({
                "id": 8,
                "subject": "Synthetic subject",
                "title": "Synthetic mark",
                "date": "01.02.2026",
                "result": "A",
                "text": "Did well.",
                "textTruncated": false,
            })
        );
        assert_eq!(
            statement_row("S", &json!({"id": 1, "markNumericValue": 4})).unwrap()["result"],
            json!("4")
        );
        assert!(statement_row("S", &json!({"id": "bad"})).is_none());
        assert!(statement_row("S", &json!({"id": 1, "markTextValue": 1})).is_none());
    }
}
