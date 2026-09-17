unsafe extern "C" {
    fn sirius_prefix_probe() -> i32;
}

pub fn probe() -> i32 {
    unsafe { sirius_prefix_probe() }
}

#[cfg(test)]
mod tests {
    #[test]
    fn links_prefix() {
        assert_eq!(super::probe(), 0);
    }
}
