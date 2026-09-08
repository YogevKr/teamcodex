use crate::pool::Pool;
use ratatui::{
    DefaultTerminal, Frame,
    crossterm::event::{self, Event, KeyCode, KeyEventKind},
    layout::{Constraint, Layout},
    style::{Color, Style},
    widgets::{Block, Paragraph, Row, Table},
};
use std::{sync::Arc, time::Duration};

pub fn draw(frame: &mut Frame, pool: &Pool) {
    let snapshot = pool.snapshot();
    let layout = Layout::vertical([
        Constraint::Length(3),
        Constraint::Min(6),
        Constraint::Length(8),
    ])
    .split(frame.area());
    frame.render_widget(
        Paragraph::new(
            "TeamCodex | account pool\nq: stop server   j/k: select   space: enable/disable",
        )
        .block(Block::bordered()),
        layout[0],
    );
    let rows = snapshot.accounts.iter().map(|a| {
        let status = if a.disabled {
            "disabled"
        } else if a.hold_until > snapshot.at {
            "waiting"
        } else if a
            .quotas
            .values()
            .any(|w| w.used_percent >= pool.config.threshold_percent)
        {
            "limited"
        } else {
            "ready"
        };
        let quota = if a.quotas.is_empty() {
            "unknown".to_owned()
        } else {
            a.quotas
                .iter()
                .map(|(id, w)| format!("{id} {:.0}%", w.used_percent))
                .collect::<Vec<_>>()
                .join(" | ")
        };
        Row::new(vec![
            a.name.clone(),
            status.to_owned(),
            a.in_flight.to_string(),
            a.requests.to_string(),
            format!("{}/{}", a.input_tokens, a.output_tokens),
            format!(
                "${:.4}{}",
                a.estimated_cost_usd,
                if a.unpriced_requests > 0 { "+?" } else { "" }
            ),
            quota,
        ])
    });
    frame.render_widget(
        Table::new(
            rows,
            [
                Constraint::Length(18),
                Constraint::Length(10),
                Constraint::Length(6),
                Constraint::Length(8),
                Constraint::Length(18),
                Constraint::Length(12),
                Constraint::Min(15),
            ],
        )
        .header(
            Row::new([
                "Account",
                "State",
                "Active",
                "Calls",
                "Tokens in/out",
                "Est. USD",
                "Quota",
            ])
            .style(Style::default().fg(Color::Cyan)),
        )
        .block(Block::bordered().title("Accounts")),
        layout[1],
    );
    let log = snapshot
        .recent
        .iter()
        .rev()
        .take(5)
        .map(|r| format!("{}  {}  {}", r.account, r.status, r.outcome))
        .collect::<Vec<_>>()
        .join("\n");
    frame.render_widget(
        Paragraph::new(log).block(Block::bordered().title("Recent requests")),
        layout[2],
    );
}

pub fn run(pool: Arc<Pool>, stop: tokio::sync::watch::Sender<bool>) -> anyhow::Result<()> {
    let mut terminal = ratatui::try_init()?;
    let result = run_inner(&mut terminal, &pool, &stop);
    ratatui::restore();
    result
}

fn run_inner(
    terminal: &mut DefaultTerminal,
    pool: &Pool,
    stop: &tokio::sync::watch::Sender<bool>,
) -> anyhow::Result<()> {
    let mut selected = 0;
    while !*stop.borrow() {
        // Accounts can be appended by a configuration reload; the list never shrinks.
        selected = selected.min(pool.len().saturating_sub(1));
        terminal.draw(|frame| {
            draw(frame, pool);
            let name = pool.account(selected).map(|a| a.name).unwrap_or_default();
            let area = frame.area();
            if area.height > 0 {
                frame.render_widget(
                    Paragraph::new(format!("Selected: {name}")),
                    ratatui::layout::Rect::new(1, area.height - 1, area.width.saturating_sub(2), 1),
                );
            }
        })?;
        if event::poll(Duration::from_millis(250))?
            && let Event::Key(key) = event::read()?
        {
            if key.kind != KeyEventKind::Press {
                continue;
            }
            match key.code {
                KeyCode::Char('q') => {
                    let _ = stop.send(true);
                    break;
                }
                KeyCode::Char('j') | KeyCode::Down => selected = (selected + 1) % pool.len().max(1),
                KeyCode::Char('k') | KeyCode::Up => selected = selected.saturating_sub(1),
                KeyCode::Char(' ') => {
                    if let Some(account) = pool.snapshot().accounts.get(selected) {
                        pool.set_enabled(&account.name, account.disabled);
                    }
                }
                _ => {}
            }
        }
    }
    Ok(())
}
