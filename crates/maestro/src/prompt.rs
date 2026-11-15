use std::future::Future;
use std::io::IsTerminal;

/// An option's value is what the command uses; its label is what the picker shows.
pub struct Choice {
    pub value: String,
    pub label: String,
}

pub fn interactive() -> bool {
    std::io::stdin().is_terminal() && std::io::stdout().is_terminal()
}

pub fn text(label: &str, value: Option<String>) -> anyhow::Result<String> {
    match value {
        Some(value) => Ok(value),
        None => {
            require_terminal(label)?;
            Ok(inquire::Text::new(label).prompt()?)
        }
    }
}

pub fn count(label: &str, value: Option<u32>) -> anyhow::Result<u32> {
    match value {
        Some(value) => Ok(value),
        None => {
            require_terminal(label)?;
            Ok(inquire::CustomType::<u32>::new(label).prompt()?)
        }
    }
}

/// Uses `value` when it was passed; otherwise loads the options and lets the user pick one.
pub async fn choose(
    label: &str,
    value: Option<String>,
    options: impl Future<Output = anyhow::Result<Vec<Choice>>>,
) -> anyhow::Result<String> {
    match value {
        Some(value) => Ok(value),
        None => {
            require_terminal(label)?;
            let mut options = options.await?;
            anyhow::ensure!(
                !options.is_empty(),
                "there is no {} to choose from",
                label.to_lowercase()
            );
            let picked = inquire::Select::new(
                label,
                options.iter().map(|choice| choice.label.clone()).collect(),
            )
            .raw_prompt()?;
            Ok(options.swap_remove(picked.index).value)
        }
    }
}

fn require_terminal(label: &str) -> anyhow::Result<()> {
    anyhow::ensure!(
        interactive(),
        "missing {}; pass it as an argument or run in a terminal to be asked",
        label.to_lowercase()
    );
    Ok(())
}
