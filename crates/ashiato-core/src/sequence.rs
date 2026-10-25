use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet, HashMap};

use chrono::{DateTime, Utc};

use crate::SignalRow;
use crate::rule::{ScenarioRule, ScenarioStep};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObservedSignal {
    pub signal_id: String,
    pub row: SignalRow,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScenarioMatch<'a> {
    pub steps: Vec<StepMatch<'a>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StepMatch<'a> {
    pub signal_id: &'a str,
    pub signals: Vec<&'a ObservedSignal>,
}

impl ScenarioMatch<'_> {
    pub fn start_time(&self) -> DateTime<Utc> {
        self.signals()
            .map(|signal| signal.row.start_time)
            .min()
            .expect("a match has at least one signal")
    }

    pub fn end_time(&self) -> DateTime<Utc> {
        self.signals()
            .map(|signal| signal.row.end_time)
            .max()
            .expect("a match has at least one signal")
    }

    fn signals(&self) -> impl Iterator<Item = &ObservedSignal> {
        self.steps
            .iter()
            .flat_map(|step| step.signals.iter().copied())
    }
}

pub fn match_scenario<'a>(
    scenario: &'a ScenarioRule,
    signals: &'a [ObservedSignal],
) -> Vec<ScenarioMatch<'a>> {
    let matcher = Matcher::new(scenario, signals);
    let mut matches = Vec::new();
    let mut previous: Option<BTreeSet<usize>> = None;
    let mut after = None;
    while let Ok(groups) = matcher.search(0, &Cursor::start(after)) {
        let used: BTreeSet<usize> = groups.iter().flatten().copied().collect();
        let Some(&first) = used.first() else {
            break;
        };
        after = Some(first);
        if previous
            .as_ref()
            .is_some_and(|previous| used.is_subset(previous))
        {
            continue;
        }
        matches.push(matcher.to_match(&groups));
        previous = Some(used);
    }
    matches
}

type Bindings<'a> = BTreeMap<&'a str, &'a str>;

#[derive(Debug, Clone, Copy)]
struct Failure {
    window_limited: bool,
}

// A failed search depends on the start of the match only through the scenario window, and
// on the end of the previous step only through `max_gap`. When neither limited the search,
// it also fails from any later position with the same bindings, because the candidates from
// there are a subset of the ones already tried. The earliest such position is remembered so
// that restarting the search from later signals does not repeat the same work.
#[derive(PartialEq, Eq, Hash)]
struct FailureKey<'a> {
    step_index: usize,
    last_end: Option<DateTime<Utc>>,
    bindings: Bindings<'a>,
}

impl<'a> FailureKey<'a> {
    fn new(step: &ScenarioStep, step_index: usize, cursor: &Cursor<'a>) -> Self {
        Self {
            step_index,
            last_end: step.max_gap.and(cursor.last_end),
            bindings: cursor.bindings.clone(),
        }
    }
}

#[derive(Clone)]
struct Cursor<'a> {
    after: Option<usize>,
    last_end: Option<DateTime<Utc>>,
    first_start: Option<DateTime<Utc>>,
    bindings: Bindings<'a>,
}

impl Cursor<'_> {
    fn start(after: Option<usize>) -> Self {
        Self {
            after,
            last_end: None,
            first_start: None,
            bindings: BTreeMap::new(),
        }
    }
}

struct Matcher<'a> {
    scenario: &'a ScenarioRule,
    signals: Vec<&'a ObservedSignal>,
    by_signal: HashMap<&'a str, Vec<usize>>,
    by_value: HashMap<(&'a str, &'a str, &'a str), Vec<usize>>,
    failures: RefCell<HashMap<FailureKey<'a>, Option<usize>>>,
}

impl<'a> Matcher<'a> {
    fn new(scenario: &'a ScenarioRule, signals: &'a [ObservedSignal]) -> Self {
        let mut sorted: Vec<&ObservedSignal> = signals.iter().collect();
        sorted.sort_by_key(|signal| signal.row.start_time);
        let mut by_signal: HashMap<&str, Vec<usize>> = HashMap::new();
        let mut by_value: HashMap<(&str, &str, &str), Vec<usize>> = HashMap::new();
        for (index, signal) in sorted.iter().enumerate() {
            by_signal.entry(&signal.signal_id).or_default().push(index);
            for (attr, value) in &signal.row.attrs {
                by_value
                    .entry((&signal.signal_id, attr, value))
                    .or_default()
                    .push(index);
            }
        }
        Self {
            scenario,
            signals: sorted,
            by_signal,
            by_value,
            failures: RefCell::new(HashMap::new()),
        }
    }

    fn to_match(&self, groups: &[Vec<usize>]) -> ScenarioMatch<'a> {
        let steps = self
            .scenario
            .steps
            .iter()
            .zip(groups)
            .map(|(step, group)| StepMatch {
                signal_id: &step.signal,
                signals: group.iter().map(|&index| self.signals[index]).collect(),
            })
            .collect();
        ScenarioMatch { steps }
    }

    // Depth-first search over the candidates of each step (design document section 9.2).
    // A greedy choice of the earliest candidate would miss matches whose `same` binding only
    // fits a later candidate.
    fn search(&self, step_index: usize, cursor: &Cursor<'a>) -> Result<Vec<Vec<usize>>, Failure> {
        let Some(step) = self.scenario.steps.get(step_index) else {
            return Ok(Vec::new());
        };
        let key = FailureKey::new(step, step_index, cursor);
        if let Some(&failed_after) = self.failures.borrow().get(&key)
            && failed_after <= cursor.after
        {
            return Err(Failure {
                window_limited: false,
            });
        }
        let result = self.search_step(step_index, cursor);
        if let Err(Failure {
            window_limited: false,
        }) = result
        {
            self.failures
                .borrow_mut()
                .entry(key)
                .and_modify(|failed_after| *failed_after = (*failed_after).min(cursor.after))
                .or_insert(cursor.after);
        }
        result
    }

    fn search_step(
        &self,
        step_index: usize,
        cursor: &Cursor<'a>,
    ) -> Result<Vec<Vec<usize>>, Failure> {
        let step = &self.scenario.steps[step_index];
        let mut window_limited = false;
        if !self.remaining_steps_are_feasible(step_index, cursor) {
            return Err(Failure { window_limited });
        }
        for &first in self.candidates(step_index, cursor) {
            let signal = self.signals[first];
            if self.exceeds_window(signal, cursor) {
                window_limited = true;
                break;
            }
            if let (Some(max_gap), Some(last_end)) = (step.max_gap, cursor.last_end)
                && signal.row.start_time - last_end > max_gap.to_time_delta()
            {
                break;
            }
            let Some(bindings) = self.bind(step_index, signal, &cursor.bindings) else {
                continue;
            };
            let first_start = cursor.first_start.unwrap_or(signal.row.start_time);
            let group = match self.group(step_index, first, first_start, &bindings) {
                Ok(group) => group,
                Err(failure) => {
                    window_limited |= failure.window_limited;
                    continue;
                }
            };
            let last = *group.last().expect("a group has at least one signal");
            let next = Cursor {
                after: Some(last),
                last_end: Some(self.signals[last].row.end_time),
                first_start: Some(first_start),
                bindings,
            };
            match self.search(step_index + 1, &next) {
                Ok(mut rest) => {
                    rest.insert(0, group);
                    return Ok(rest);
                }
                Err(failure) => window_limited |= failure.window_limited,
            }
        }
        if step.optional {
            match self.search(step_index + 1, cursor) {
                Ok(mut rest) => {
                    rest.insert(0, Vec::new());
                    return Ok(rest);
                }
                Err(failure) => window_limited |= failure.window_limited,
            }
        }
        Err(Failure { window_limited })
    }

    // Collects the signals that make up a step from `first` onward until `min_count` is met.
    // With `distinct_by`, only signals that add a new attribute value are counted and kept.
    fn group(
        &self,
        step_index: usize,
        first: usize,
        first_start: DateTime<Utc>,
        bindings: &Bindings<'a>,
    ) -> Result<Vec<usize>, Failure> {
        let step = &self.scenario.steps[step_index];
        let required = usize::try_from(step.min_count.max(1)).unwrap_or(usize::MAX);
        let distinct_value = |index: usize| match &step.distinct_by {
            Some(attr) => self.signals[index].row.attrs.get(attr).map(String::as_str),
            None => Some(""),
        };
        let mut seen = BTreeSet::new();
        let mut group = Vec::new();
        let mut window_limited = false;
        let indices = &self.by_signal[step.signal.as_str()];
        let start = indices.partition_point(|&index| index < first);
        for &index in &indices[start..] {
            if group.len() == required {
                break;
            }
            let signal = self.signals[index];
            if signal.row.end_time - first_start > self.scenario.window.to_time_delta() {
                window_limited = true;
                break;
            }
            if self.bind(step_index, signal, bindings).is_none() {
                continue;
            }
            let Some(value) = distinct_value(index) else {
                continue;
            };
            if step.distinct_by.is_none() || seen.insert(value) {
                group.push(index);
            }
        }
        if group.len() == required {
            Ok(group)
        } else {
            Err(Failure { window_limited })
        }
    }

    fn bind(
        &self,
        step_index: usize,
        signal: &'a ObservedSignal,
        bindings: &Bindings<'a>,
    ) -> Option<Bindings<'a>> {
        let mut bindings = bindings.clone();
        for attr in &self.scenario.steps[step_index].same {
            let value = signal.row.attrs.get(attr)?;
            let bound = *bindings.entry(attr).or_insert(value);
            if bound != value {
                return None;
            }
        }
        Some(bindings)
    }

    fn exceeds_window(&self, signal: &ObservedSignal, cursor: &Cursor<'_>) -> bool {
        cursor.first_start.is_some_and(|first_start| {
            signal.row.start_time - first_start > self.scenario.window.to_time_delta()
        })
    }

    // Candidates of a step come after the previous step, in start_time order. When the step
    // has a `same` attribute that is already bound, only signals with that value are listed,
    // so that unmatched values do not multiply the search.
    fn candidates(&self, step_index: usize, cursor: &Cursor<'a>) -> &[usize] {
        let step = &self.scenario.steps[step_index];
        let bound = step.same.iter().find_map(|attr| {
            let value = cursor.bindings.get(attr.as_str())?;
            Some((step.signal.as_str(), attr.as_str(), *value))
        });
        let indices = match bound {
            Some(key) => self.by_value.get(&key),
            None => self.by_signal.get(step.signal.as_str()),
        };
        let indices = indices.map(Vec::as_slice).unwrap_or_default();
        let start = cursor
            .after
            .map_or(0, |after| indices.partition_point(|&index| index <= after));
        &indices[start..]
    }

    fn remaining_steps_are_feasible(&self, step_index: usize, cursor: &Cursor<'a>) -> bool {
        (step_index..self.scenario.steps.len())
            .filter(|&index| !self.scenario.steps[index].optional)
            .all(|index| !self.candidates(index, cursor).is_empty())
    }
}
