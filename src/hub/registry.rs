use std::{
    collections::{HashMap, HashSet},
    hash::Hash,
};

#[derive(Debug)]
pub struct Registry<T> {
    topics: HashMap<String, HashSet<T>>,
}

impl<T> Default for Registry<T> {
    fn default() -> Self {
        Self { topics: HashMap::new() }
    }
}

impl<T> Registry<T>
where
    T: Eq + Hash,
{
    /// return true if the topic is new
    pub fn subscribe(&mut self, topic: &str, leg_id: T) -> bool {
        self.topics.entry(topic.to_string()).or_default().insert(leg_id)
    }

    /// return true if the topic exists
    pub fn unsubscribe(&mut self, topic: &str, leg_id: T) -> bool {
        if let Some(legs) = self.topics.get_mut(topic) {
            legs.remove(&leg_id)
        } else {
            false
        }
    }

    pub fn get(&self, topic: &str) -> Option<impl Iterator<Item = &T>> {
        self.topics.get(topic).map(|legs| legs.iter())
    }
}
