pub fn websocket_origin_allowed(headers: &[&str]) -> bool {
    let Some(origin) = header(headers, "origin") else {
        return true;
    };
    let Some(host) = header(headers, "host") else {
        return false;
    };
    origin.eq_ignore_ascii_case(&format!("https://{host}"))
}

fn header<'a>(headers: &'a [&'a str], name: &str) -> Option<&'a str> {
    headers.iter().find_map(|line| {
        let (header, value) = line.split_once(':')?;
        header
            .trim()
            .eq_ignore_ascii_case(name)
            .then(|| value.trim())
    })
}

#[cfg(test)]
mod tests {
    use super::websocket_origin_allowed;

    #[test]
    fn websocket_origin_must_match_host() {
        assert!(websocket_origin_allowed(&[
            "Host: caster.example",
            "Origin: https://caster.example",
        ]));
        assert!(websocket_origin_allowed(&["Host: caster.example"]));
        assert!(!websocket_origin_allowed(&[
            "Host: caster.example",
            "Origin: https://attacker.example",
        ]));
        assert!(!websocket_origin_allowed(&[
            "Host: caster.example",
            "Origin: http://caster.example",
        ]));
        assert!(!websocket_origin_allowed(&[
            "Origin: https://caster.example"
        ]));
    }
}
