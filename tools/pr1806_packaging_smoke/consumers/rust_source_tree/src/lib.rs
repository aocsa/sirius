unsafe extern "C" {
    fn sirius_source_tree_probe() -> i32;
}

pub fn probe() -> i32 {
    unsafe { sirius_source_tree_probe() }
}

#[cfg(test)]
mod tests {
    #[test]
    fn links_source_tree() {
        assert_eq!(super::probe(), 0);
    }
}
