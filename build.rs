fn main() {
    // extension-module 构建（maturin 发布 wheel）时，pyo3 会自行决定不链接
    // libpython（扩展模块的符号在运行时从宿主解释器解析）。我们绝不能在这里
    // 再输出 link-lib——否则 wheel 会携带 libpython 依赖，manylinux 审计直接
    // 失败（libpython 不在允许列表），本地构建也会产出装上就崩的 wheel。
    //
    // 判断信号与 pyo3-build-config 的 is_extension_module() 完全一致：
    //   - CARGO_FEATURE_EXTENSION_MODULE：Cargo.toml 的 extension-module feature
    //     （maturin 构建时 default features 会启用它）
    //   - PYO3_BUILD_EXTENSION_MODULE：maturin 构建时总是设置的环境变量
    // 两种信号都不存在，说明是 cargo test / cargo bench 这类需要链接
    // libpython 的 standalone 二进制构建，此时显式链接才有意义。
    let is_extension_module = std::env::var("CARGO_FEATURE_EXTENSION_MODULE").is_ok()
        || std::env::var("PYO3_BUILD_EXTENSION_MODULE").is_ok();
    if is_extension_module {
        return;
    }

    // 非 extension-module 构建（cargo test --features test-init 等）：
    // 测试二进制需要解析 Python C API 符号，必须链接 libpython。
    // pyo3 在非 extension-module 模式下同样会链接，这里保持历史行为，
    // 不改变 cargo test 的既有行为。
    let config = pyo3_build_config::get();
    if let Some(lib_dir) = config.lib_dir() {
        println!("cargo:rustc-link-search=native={}", lib_dir);
    }
    if let Some(lib_name) = config.lib_name() {
        println!("cargo:rustc-link-lib={}", lib_name);
    }
}
