#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Token {
    Word(String),
    Phrase(String),
    Colon,
    LeftParen,
    RightParen,
    LeftBracket,
    RightBracket,
    Minus,
    And,
    Or,
    Not,
    To,
}

pub(crate) fn tokenize(value: &str) -> Result<Vec<Token>, String> {
    let characters = value.chars().collect::<Vec<_>>();
    let mut tokens = Vec::new();
    let mut cursor = 0;
    while let Some(&character) = characters.get(cursor) {
        if character.is_whitespace() {
            cursor += 1;
            continue;
        }
        if let Some(token) = single_token(character) {
            tokens.push(token);
            cursor += 1;
            continue;
        }
        if character == '"' {
            let (phrase, next) = read_quoted(&characters, cursor + 1)?;
            tokens.push(Token::Phrase(phrase));
            cursor = next;
            continue;
        }

        let mut word = String::new();
        while let Some(&character) = characters.get(cursor) {
            if character.is_whitespace() || matches!(character, ':' | '(' | ')' | '[' | ']') {
                break;
            }
            if character == '\\' {
                cursor += 1;
                let escaped = characters
                    .get(cursor)
                    .ok_or_else(|| "log query cannot end with an escape".to_string())?;
                word.push(*escaped);
            } else {
                word.push(character);
            }
            cursor += 1;
        }
        if word.is_empty() {
            return Err("log query contains an empty term".to_string());
        }
        tokens.push(keyword_or_word(word));
    }
    Ok(tokens)
}

fn single_token(character: char) -> Option<Token> {
    match character {
        ':' => Some(Token::Colon),
        '(' => Some(Token::LeftParen),
        ')' => Some(Token::RightParen),
        '[' => Some(Token::LeftBracket),
        ']' => Some(Token::RightBracket),
        '-' => Some(Token::Minus),
        _ => None,
    }
}

fn keyword_or_word(word: String) -> Token {
    match word.as_str() {
        "AND" => Token::And,
        "OR" => Token::Or,
        "NOT" => Token::Not,
        "TO" => Token::To,
        _ => Token::Word(word),
    }
}

fn read_quoted(characters: &[char], mut cursor: usize) -> Result<(String, usize), String> {
    let mut value = String::new();
    while let Some(&character) = characters.get(cursor) {
        match character {
            '"' => return Ok((value, cursor + 1)),
            '\\' => {
                cursor += 1;
                let escaped = characters
                    .get(cursor)
                    .ok_or_else(|| "quoted log query cannot end with an escape".to_string())?;
                value.push(*escaped);
            }
            character => value.push(character),
        }
        cursor += 1;
    }
    Err("unterminated quoted phrase in log query".to_string())
}

pub(crate) fn starts_expression(token: &Token) -> bool {
    matches!(
        token,
        Token::Word(_) | Token::Phrase(_) | Token::LeftParen | Token::Minus | Token::Not
    )
}

pub(crate) fn label(token: &Token) -> String {
    match token {
        Token::Word(value) | Token::Phrase(value) => format!("`{value}`"),
        Token::Colon => "`:`".to_string(),
        Token::LeftParen => "`(`".to_string(),
        Token::RightParen => "`)`".to_string(),
        Token::LeftBracket => "`[`".to_string(),
        Token::RightBracket => "`]`".to_string(),
        Token::Minus => "`-`".to_string(),
        Token::And => "`AND`".to_string(),
        Token::Or => "`OR`".to_string(),
        Token::Not => "`NOT`".to_string(),
        Token::To => "`TO`".to_string(),
    }
}
