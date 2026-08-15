// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

#[derive(Clone, Copy, Debug)]
#[repr(i32)]
pub enum ControlStatePrecision {
    OneSecond = 0,
    FiveSeconds = 1,
    TenSeconds = 2,
    OneMinute = 3,
    FiveMinutes = 4,
    TenMinutes = 5,
    OneHour = 6,
    OneDay = 7,
    OneMonth = 8,
}

#[derive(Clone, Copy, Debug)]
#[repr(i32)]
pub enum ControlStateWindowUnit {
    Second = 0,
    Minute = 1,
    Hour = 2,
    Day = 3,
}

#[derive(Clone, Copy, Debug)]
pub struct ControlStateWindow {
    pub start: i64,
    pub end: i64,
    pub unit: ControlStateWindowUnit,
}

impl Default for ControlStateWindow {
    fn default() -> Self {
        Self {
            start: -1,
            end: 0,
            unit: ControlStateWindowUnit::Hour,
        }
    }
}

#[derive(Clone, Copy, Debug)]
#[cfg_attr(feature = "proxy", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "proxy", serde(rename_all = "snake_case"))]
pub enum ControlStateHType {
    Count,
    Min,
    Max,
    Change,
}

#[derive(Clone, Copy, Debug)]
#[cfg_attr(feature = "proxy", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "proxy", serde(rename_all = "snake_case"))]
pub enum ControlStateFolType {
    First,
    Last,
}
