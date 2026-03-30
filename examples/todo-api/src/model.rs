use serde::{Deserialize, Serialize};

pub const MILLIS_PER_DAY: u64 = 24 * 60 * 60 * 1000;
pub const DAYS_PER_WEEK: u64 = 7;
pub const MILLIS_PER_WEEK: u64 = DAYS_PER_WEEK * MILLIS_PER_DAY;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TodoStatus {
    Open,
    Completed,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct TodoRecord {
    pub todo_id: String,
    pub title: String,
    pub notes: String,
    pub status: TodoStatus,
    pub scheduled_for_day: Option<u64>,
    pub placeholder: bool,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct CreateTodoRequest {
    pub todo_id: String,
    pub title: String,
    #[serde(default)]
    pub notes: String,
    pub scheduled_for_day: Option<u64>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct UpdateTodoRequest {
    pub title: Option<String>,
    pub notes: Option<String>,
    pub scheduled_for_day: Option<u64>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct PlannerCommand {
    pub todo_id: String,
    pub title: String,
    pub scheduled_for_day: u64,
    pub created_at_ms: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct PlannerState {
    pub next_fire_at_ms: u64,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct PlannerSchedule {
    pub day_millis: u64,
    pub days_per_week: u64,
}

impl PlannerSchedule {
    pub const fn new(day_millis: u64, days_per_week: u64) -> Self {
        Self {
            day_millis,
            days_per_week,
        }
    }

    pub const fn week_millis(self) -> u64 {
        self.day_millis.saturating_mul(self.days_per_week)
    }

    pub fn next_week_start_ms(self, now_ms: u64) -> u64 {
        now_ms
            .checked_div(self.week_millis())
            .and_then(|week| week.checked_add(1))
            .and_then(|week| week.checked_mul(self.week_millis()))
            .unwrap_or(self.week_millis())
    }

    pub fn day_index(self, scheduled_for_day: u64) -> u64 {
        scheduled_for_day / self.day_millis.max(1)
    }

    pub fn placeholder_todo_id(self, scheduled_for_day: u64) -> String {
        format!("placeholder:{}", self.day_index(scheduled_for_day))
    }
}

impl Default for PlannerSchedule {
    fn default() -> Self {
        Self::new(MILLIS_PER_DAY, DAYS_PER_WEEK)
    }
}

impl TodoRecord {
    pub fn placeholder_for_day(scheduled_for_day: u64, created_at_ms: u64) -> Self {
        Self {
            todo_id: placeholder_todo_id(scheduled_for_day),
            title: format!("Plan day {}", scheduled_for_day / MILLIS_PER_DAY),
            notes: String::new(),
            status: TodoStatus::Open,
            scheduled_for_day: Some(scheduled_for_day),
            placeholder: true,
            created_at_ms,
            updated_at_ms: created_at_ms,
        }
    }
}

pub fn day_start_ms(day_index: u64) -> u64 {
    day_index.saturating_mul(MILLIS_PER_DAY)
}

pub fn next_week_start_ms(now_ms: u64) -> u64 {
    PlannerSchedule::default().next_week_start_ms(now_ms)
}

pub fn placeholder_todo_id(scheduled_for_day: u64) -> String {
    PlannerSchedule::default().placeholder_todo_id(scheduled_for_day)
}
