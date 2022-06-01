pub fn sorted_lines(text: &str) -> Vec<String> {
    let mut lines: Vec<String> = text
        .split("\n")
        .map(|l| l.trim().to_string())
        .filter(|l| l.len() > 0)
        .collect();
    lines.sort();
    lines
}
