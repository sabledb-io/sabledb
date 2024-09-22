#[derive(Default, Debug)]
pub enum MatcherType {
    WildCard(wildmatch::WildMatch),
    Prefix,
    #[default]
    PassThrough,
}

#[derive(Default, Debug)]
pub struct PatternMatcher<'a> {
    pattern: Option<&'a [u8]>,
    matcher: MatcherType,
}

impl<'a> PatternMatcher<'a> {
    /// Returns true if pattern applies to the given input string ("what")
    pub fn matches(&self, input_string: &'a [u8]) -> bool {
        match &self.matcher {
            MatcherType::WildCard(matcher) => {
                matcher.matches(String::from_utf8_lossy(input_string).to_string().as_str())
            }
            MatcherType::Prefix => {
                let Some(pattern) = self.pattern else {
                    return false;
                };
                input_string.starts_with(pattern)
            }
            MatcherType::PassThrough => true,
        }
    }

    pub fn builder() -> PatternMatcherBuilder<'a> {
        PatternMatcherBuilder::default()
    }
}

#[derive(Default)]
pub struct PatternMatcherBuilder<'a> {
    pattern: Option<&'a [u8]>,
    matcher: MatcherType,
}

impl<'a> PatternMatcherBuilder<'a> {
    /// Create a prefix matcher
    pub fn prefix(mut self, pattern: &'a [u8]) -> Self {
        self.matcher = MatcherType::Prefix;
        self.pattern = Some(pattern);
        self
    }

    /// Create
    pub fn wildcard(mut self, pattern: &'a [u8]) -> Self {
        self.matcher = MatcherType::WildCard(wildmatch::WildMatch::new(
            String::from_utf8_lossy(pattern).to_string().as_str(),
        ));
        self.pattern = Some(pattern);
        self
    }

    pub fn pass_through(mut self) -> Self {
        self.matcher = MatcherType::PassThrough;
        self
    }

    /// Consume the builder and construct a PatternMatcher
    pub fn build(self) -> PatternMatcher<'a> {
        PatternMatcher {
            pattern: self.pattern,
            matcher: self.matcher,
        }
    }
}

//  _    _ _   _ _____ _______      _______ ______  _____ _______ _____ _   _  _____
// | |  | | \ | |_   _|__   __|    |__   __|  ____|/ ____|__   __|_   _| \ | |/ ____|
// | |  | |  \| | | |    | |    _     | |  | |__  | (___    | |    | | |  \| | |  __|
// | |  | | . ` | | |    | |   / \    | |  |  __|  \___ \   | |    | | | . ` | | |_ |
// | |__| | |\  |_| |_   | |   \_/    | |  | |____ ____) |  | |   _| |_| |\  | |__| |
//  \____/|_| \_|_____|  |_|          |_|  |______|_____/   |_|  |_____|_| \_|\_____|
//
#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_pass_through() {
        let matcher = PatternMatcher::builder().pass_through().build();
        assert!(matcher.matches(b"hello")); // always return true
    }

    #[test]
    fn test_prefix_matcher() {
        let matcher = PatternMatcher::builder().prefix(b"hello").build();
        assert!(matcher.matches(b"hello world"));
        assert!(!matcher.matches(b"ello"));
    }

    #[test]
    fn test_wildcard_matcher_star() {
        let matcher = PatternMatcher::builder().wildcard(b"*hello*").build();
        assert!(matcher.matches(b"helloworld"));
        assert!(matcher.matches(b"hello_world"));
        assert!(matcher.matches(b"__hello__"));
    }

    #[test]
    fn test_wildcard_matcher_question_mark() {
        let matcher = PatternMatcher::builder().wildcard(b"?hello").build();
        assert!(matcher.matches(b"bhello"));
        assert!(!matcher.matches(b"hello_world"));
        assert!(matcher.matches(b"_hello"));
    }
}
