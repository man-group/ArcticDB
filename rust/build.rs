fn main() {
    if let Ok(path) = std::env::var("ARCTICDB_NATIVE_PATH") {
        println!("cargo:rustc-link-search=native={path}");
        println!("cargo:rustc-link-arg=-Wl,-rpath,{path}");
    }
    println!("cargo:rustc-link-lib=dylib=arcticdb_c");
}
