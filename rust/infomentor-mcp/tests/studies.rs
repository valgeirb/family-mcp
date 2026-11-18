//! Learning-unit and assessment reads against a local fake upstream. Needs `test-origin`.
//! All data is synthetic. The signed-in InfoMentor shapes are not exercised here.
#![cfg(feature = "test-origin")]

mod scratch;

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpListener;
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};
use std::sync::{Arc, Mutex};

use serde_json::{Value, json};

/// What the learning-unit and assessment reads answer.
#[derive(Clone, Copy, PartialEq)]
enum Mode {
    /// App data names the read URLs and the detail calls succeed.
    Ready,
    /// App data has the list and no detail address.
    NoAddress,
    /// The detail calls answer 404.
    Missing,
    /// App data is not the object the tools require.
    Bad,
}

const HOME: &str = r#"<script>IMHome.home.homeData = {"account":{"currentUser":{"id":"parent"},"pupils":[{"id":"child","name":"Child","selected":true,"switchPupilUrl":null}]},"apps":[]}; IMHome.home.init(IMHome.home.homeData);</script>"#;

const LEARNING: &str = r#"{"subjects":[{"id":4,"name":"Synthetic subject"}],"uols":[
  {"id":1000001,"title":"Synthetic unit","subjects":[4],"description":"<p>Short</p>"},
  {"id":"no"},
  {"id":1000002,"title":"Second","subjects":[{"name":"Named"}]}
],"urls":{"getUol":"/uolv2/uolv2/GetUol","getUols":"/uolv2/uolv2/GetUols","persistFilter":"/uolv2/uolv2/PersistFilter"}}"#;

const UNIT: &str = r#"{"id":1000001,"title":"Synthetic unit","subjects":[{"id":4,"name":"Synthetic subject"}],"sections":[
  {"type":"uol","title":"About","overview":[
    {"label":"Lýsing","value":"<p>Learn <b>this</b>.</p>"},
    {"label":"Tímabil","value":"1.1.2026–1.6.2026"}
  ]},
  {"type":"syllabus","sections":[{"title":"Goals","fields":[{"label":"Aim","value":"<p>Read</p>"}]}]},
  {"type":"objective","objectives":["<p>Count</p>"]},
  {"type":"statement","matrixTypes":[{"title":"Criteria","matrices":[{"title":"Level","statements":[
    {"text":"<p>Explain</p>","levelHeader":"A","groupedStatements":[],"layers":[{"title":"Layer","freeText":"<p>More</p>"}]}
  ]}]}]},
  {"type":"timeline","entries":[{"text":"not included"}]}
]}"#;

const ASSESSMENTS: &str = r#"{"academicYearItems":[{"key":"9","title":"Synthetic year","selected":true}],
"navigationModel":{"navigationItemsList":[
  {"id":3,"title":"Synthetic subject","linkedGrids":[5],"linkedUols":[8]},
  {"id":"no","title":"Dropped"},
  {"id":6,"title":"Empty subject","linkedGrids":[],"linkedUols":[]}
]},
"urls":{"getGridItems":"/assessmentv2/assessmentv2/GetGridItems","getUolItems":"/assessmentv2/assessmentv2/GetUolItems","updateNotificationState":"/assessmentv2/assessmentv2/UpdateNotificationState"}}"#;

const STATEMENTS: &str = r#"{"statementItemsList":[
  {"id":8,"headerLabel":"Synthetic mark","text":"<p>Did <b>well</b>.</p>","markLastUpdated":"01.02.2026","markTextValue":"A","markNumericValue":4},
  {"id":"bad"}
]}"#;

fn handle(stream: std::net::TcpStream, mode: Mode, log: &Mutex<Vec<String>>) {
    let mut reader = BufReader::new(&stream);
    let mut first = String::new();
    reader.read_line(&mut first).unwrap();
    let mut length = 0;
    let mut content_type = String::new();
    loop {
        let mut line = String::new();
        reader.read_line(&mut line).unwrap();
        if line == "\r\n" || line.is_empty() {
            break;
        }
        let lower = line.to_ascii_lowercase();
        if let Some(value) = lower.strip_prefix("content-length:") {
            length = value.trim().parse().unwrap();
        }
        if let Some(value) = lower.strip_prefix("content-type:") {
            content_type = value.trim().trim_end_matches('\r').to_owned();
        }
    }
    let mut body = vec![0; length];
    reader.read_exact(&mut body).unwrap();
    let mut parts = first.split_whitespace();
    let (method, target) = (parts.next().unwrap(), parts.next().unwrap());
    let body = String::from_utf8(body).unwrap();
    log.lock().unwrap().push(
        format!("{method} {target} {content_type} {body}")
            .trim_end()
            .to_owned(),
    );

    let im1 = "/im1.infomentor.is/production/mentor/";
    let minn = "https://minn.infomentor.is";
    let learning = match mode {
        Mode::NoAddress => {
            r#"{"subjects":[],"uols":[{"id":1,"title":"Synthetic unit","subjects":[]}],"urls":{}}"#
        }
        Mode::Bad => r#"{"extra":1}"#,
        _ => LEARNING,
    };
    let assessments = match mode {
        Mode::NoAddress => {
            r#"{"academicYearItems":[{"key":"9","selected":true}],"navigationModel":{"navigationItemsList":[{"id":3,"title":"Synthetic subject","linkedGrids":[5]}]},"urls":{}}"#
        }
        Mode::Bad => r#"{}"#,
        _ => ASSESSMENTS,
    };
    let detail = if mode == Mode::Missing {
        (404, "missing")
    } else {
        (200, UNIT)
    };
    let statements = if mode == Mode::Missing {
        (404, "missing")
    } else {
        (200, STATEMENTS)
    };
    let (status, headers, body): (u16, Vec<String>, &str) = match (method, target) {
        ("GET", path) if path == im1 => (
            200,
            vec!["Set-Cookie: preflight=ok; Path=/; Secure; HttpOnly".into()],
            r#"<form method="POST" action="./"><input type="hidden" name="__VIEWSTATE" value="s"><input type="hidden" name="__EVENTVALIDATION" value="v"></form>"#,
        ),
        ("POST", path) if path == im1 && body.contains("oauth_token") => (
            303,
            vec![
                format!("Location: {minn}/Authentication/Authentication/LoginCallback"),
                "Set-Cookie: relay=ok; Path=/; Secure; HttpOnly".into(),
            ],
            "",
        ),
        ("POST", path) if path == im1 => (
            302,
            vec![format!(
                "Location: {minn}/authentication/authentication/login"
            )],
            "",
        ),
        ("GET", "/minn.infomentor.is/authentication/authentication/login") => (
            200,
            vec![],
            r#"<form id="openid_message" method="post" action="https://im1.infomentor.is/production/mentor/"><input type="hidden" name="oauth_token" value="t"></form>"#,
        ),
        ("GET", "/minn.infomentor.is/Authentication/Authentication/LoginCallback") => (
            307,
            vec![
                format!("Location: {minn}/"),
                "Set-Cookie: IMHome=ok; Path=/; Secure; HttpOnly".into(),
            ],
            "",
        ),
        ("GET", "/minn.infomentor.is/") => (200, vec![], HOME),
        ("POST", "/minn.infomentor.is/authentication/authentication/isauthenticated/") => {
            (200, vec![], "true")
        }
        ("POST", "/minn.infomentor.is/uolv2/uolv2/appData") => (200, vec![], learning),
        ("POST", "/minn.infomentor.is/uolv2/uolv2/GetUol") => (detail.0, vec![], detail.1),
        ("POST", "/minn.infomentor.is/assessmentv2/assessmentv2/appData") => {
            (200, vec![], assessments)
        }
        ("POST", path)
            if path.starts_with("/minn.infomentor.is/assessmentv2/assessmentv2/GetGridItems/") =>
        {
            (statements.0, vec![], statements.1)
        }
        _ => (404, vec![], "unexpected"),
    };
    let kind = if body.starts_with('<') {
        "text/html"
    } else {
        "application/json"
    };
    let mut head = format!(
        "HTTP/1.1 {status} X\r\nContent-Type: {kind}\r\nContent-Length: {}\r\nConnection: close\r\n",
        body.len()
    );
    for header in headers {
        head.push_str(&header);
        head.push_str("\r\n");
    }
    (&stream)
        .write_all(format!("{head}\r\n{body}").as_bytes())
        .unwrap();
}

struct Mcp {
    child: Child,
    stdin: ChildStdin,
    stdout: BufReader<ChildStdout>,
    log: Arc<Mutex<Vec<String>>>,
    next: u64,
    _home: scratch::Scratch,
}

impl Mcp {
    fn start(name: &str, mode: Mode) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let origin = format!("http://127.0.0.1:{}", listener.local_addr().unwrap().port());
        let log = Arc::new(Mutex::new(Vec::new()));
        let shared = log.clone();
        std::thread::spawn(move || {
            for stream in listener.incoming().flatten() {
                handle(stream, mode, &shared);
            }
        });
        let home = scratch::Scratch::new(&format!("studies-{name}"));
        let command = |args: &[&str]| {
            let mut command = Command::new(env!("CARGO_BIN_EXE_infomentor-mcp"));
            command
                .args(args)
                .env("INFOMENTOR_TEST_ORIGIN", &origin)
                .env_remove("INFOMENTOR_USERNAME")
                .env_remove("INFOMENTOR_PASSWORD");
            home.isolate(&mut command);
            command
        };
        let login = command(&["login"])
            .env("INFOMENTOR_USERNAME", "synthetic-user")
            .env("INFOMENTOR_PASSWORD", "synthetic-password")
            .stdin(Stdio::null())
            .output()
            .unwrap();
        assert!(login.status.success(), "{login:?}");
        let mut child = command(&["serve"])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let mut mcp = Self {
            stdin: child.stdin.take().unwrap(),
            stdout: BufReader::new(child.stdout.take().unwrap()),
            child,
            log,
            next: 1,
            _home: home,
        };
        mcp.request(
            "initialize",
            json!({"protocolVersion": "2025-11-25", "capabilities": {}, "clientInfo": {"name": "studies", "version": "1"}}),
        );
        writeln!(
            mcp.stdin,
            r#"{{"jsonrpc":"2.0","method":"notifications/initialized"}}"#
        )
        .unwrap();
        mcp
    }

    fn request(&mut self, method: &str, params: Value) -> Value {
        let id = self.next;
        self.next += 1;
        let request = json!({"jsonrpc": "2.0", "id": id, "method": method, "params": params});
        writeln!(self.stdin, "{request}").unwrap();
        loop {
            let mut line = String::new();
            assert!(self.stdout.read_line(&mut line).unwrap() > 0, "serve ended");
            let message: Value = serde_json::from_str(&line).unwrap();
            if message["id"] == id {
                return message;
            }
        }
    }

    fn call(&mut self, name: &str, arguments: Value) -> Value {
        self.request("tools/call", json!({"name": name, "arguments": arguments}))["result"].clone()
    }

    fn study_requests(&self) -> Vec<String> {
        self.log
            .lock()
            .unwrap()
            .iter()
            .filter(|request| request.contains("/uolv2/") || request.contains("/assessmentv2/"))
            .cloned()
            .collect()
    }
}

impl Drop for Mcp {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn text_of(result: &Value) -> String {
    result["content"]
        .as_array()
        .and_then(|items| items.first())
        .and_then(|item| item["text"].as_str())
        .unwrap_or_default()
        .to_owned()
}

#[test]
fn learning_units_and_assessments_are_read_as_plain_text() {
    let mut mcp = Mcp::start("ready", Mode::Ready);
    let list = mcp.call("infomentor_list_learning_units", json!({}));
    assert_eq!(
        list["structuredContent"],
        json!({
            "units": [
                {
                    "id": 1000001,
                    "title": "Synthetic unit",
                    "subjects": ["Synthetic subject"],
                    "description": "Short",
                },
                {
                    "id": 1000002,
                    "title": "Second",
                    "subjects": ["Named"],
                    "description": null,
                }
            ],
            "skipped": 1,
            "retrievedAt": list["structuredContent"]["retrievedAt"],
        }),
        "{list}"
    );

    let unit = mcp.call("infomentor_get_learning_unit", json!({"id": 1000001}));
    assert_eq!(
        unit["structuredContent"]["unit"],
        json!({
            "id": 1000001,
            "title": "Synthetic unit",
            "subjects": ["Synthetic subject"],
            "period": "1.1.2026–1.6.2026",
            "description": "About\n\nLýsing: Learn this.\nTímabil: 1.1.2026–1.6.2026",
            "goals": "Goals\nAim: Read\n\nCount",
            "criteria": "Criteria\nLevel\nA: Explain\nLayer: More",
            "truncated": false,
        }),
        "{unit}"
    );

    let assessments = mcp.call("infomentor_get_assessments", json!({}));
    assert_eq!(
        assessments["structuredContent"],
        json!({
            "assessments": [
                {
                    "id": 8,
                    "subject": "Synthetic subject",
                    "title": "Synthetic mark",
                    "date": "01.02.2026",
                    "result": "A",
                    "text": "Did well.",
                    "textTruncated": false,
                },
                {
                    "id": 6,
                    "subject": "Empty subject",
                    "title": "Empty subject",
                    "date": null,
                    "result": null,
                    "text": "",
                    "textTruncated": false,
                }
            ],
            "skipped": 2,
            "retrievedAt": assessments["structuredContent"]["retrievedAt"],
        }),
        "{assessments}"
    );
    let one = mcp.call("infomentor_get_assessments", json!({"id": 3}));
    assert_eq!(
        one["structuredContent"]["assessments"]
            .as_array()
            .unwrap()
            .len(),
        1,
        "{one}"
    );
    let missing = mcp.call("infomentor_get_assessments", json!({"id": 99}));
    assert_eq!(missing["isError"], true, "{missing}");
    assert!(
        text_of(&missing).contains("not in the current list"),
        "{missing}"
    );

    let requests = mcp.study_requests();
    assert!(
        requests.iter().any(|request| request
            == "POST /minn.infomentor.is/uolv2/uolv2/GetUol application/json; charset=utf-8 {\"id\":1000001}"),
        "{requests:?}"
    );
    assert!(
        requests.iter().any(|request| request
            == "POST /minn.infomentor.is/assessmentv2/assessmentv2/GetGridItems/5?academicYearId=9 application/json; charset=utf-8"),
        "{requests:?}"
    );
    assert!(
        requests.iter().any(|request| request.starts_with(
            "POST /minn.infomentor.is/uolv2/uolv2/appData application/x-www-form-urlencoded"
        )),
        "{requests:?}"
    );
    assert!(
        requests
            .iter()
            .all(|request| !request.contains("PersistFilter")
                && !request.contains("GetUols")
                && !request.contains("UpdateNotificationState")
                && !request.contains("GetUolItems")),
        "{requests:?}"
    );
}

#[test]
fn a_missing_detail_address_is_an_error_and_is_not_guessed() {
    let mut mcp = Mcp::start("no-address", Mode::NoAddress);
    let unit = mcp.call("infomentor_get_learning_unit", json!({"id": 1}));
    assert_eq!(unit["isError"], true, "{unit}");
    assert!(
        text_of(&unit).contains("did not name a learning-unit address"),
        "{unit}"
    );
    let assessments = mcp.call("infomentor_get_assessments", json!({"id": 3}));
    assert_eq!(assessments["isError"], true, "{assessments}");
    assert!(
        text_of(&assessments).contains("did not name an assessment address"),
        "{assessments}"
    );
    assert!(
        mcp.study_requests()
            .iter()
            .all(|request| request.contains("/appData")),
        "{:?}",
        mcp.study_requests()
    );
}

#[test]
fn a_missing_detail_page_is_a_clear_error() {
    let mut mcp = Mcp::start("missing", Mode::Missing);
    let unit = mcp.call("infomentor_get_learning_unit", json!({"id": 1000001}));
    assert_eq!(unit["isError"], true, "{unit}");
    assert!(
        text_of(&unit).contains("did not return this learning unit"),
        "{unit}"
    );
    let assessments = mcp.call("infomentor_get_assessments", json!({}));
    assert_eq!(assessments["isError"], true, "{assessments}");
    assert!(
        text_of(&assessments).contains("did not return this assessment"),
        "{assessments}"
    );
}

#[test]
fn an_unexpected_list_is_an_error() {
    let mut mcp = Mcp::start("bad", Mode::Bad);
    let list = mcp.call("infomentor_list_learning_units", json!({}));
    assert_eq!(list["isError"], true, "{list}");
    assert!(text_of(&list).contains("format has changed"), "{list}");
    let assessments = mcp.call("infomentor_get_assessments", json!({}));
    assert_eq!(assessments["isError"], true, "{assessments}");
    assert!(
        text_of(&assessments).contains("format has changed"),
        "{assessments}"
    );
}

#[test]
fn invalid_study_input_is_refused_before_any_request() {
    let mut mcp = Mcp::start("invalid", Mode::Ready);
    for (name, arguments) in [
        ("infomentor_get_learning_unit", json!({})),
        ("infomentor_get_learning_unit", json!({"id": 0})),
        ("infomentor_list_learning_units", json!({"id": 1})),
        ("infomentor_get_assessments", json!({"id": -1})),
        ("infomentor_get_assessments", json!({"extra": true})),
    ] {
        let answer = mcp.request("tools/call", json!({"name": name, "arguments": arguments}));
        let refused = answer["result"]["isError"] == true || answer.get("error").is_some();
        assert!(refused, "{name} {arguments}: {answer}");
    }
    assert!(mcp.study_requests().is_empty());
}
