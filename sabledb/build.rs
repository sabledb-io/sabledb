#[cfg(target_os = "windows")]
fn main() {
    println!("cargo:rustc-link-lib=pthread");
}

#[cfg(not(target_os = "windows"))]
fn main() {}
