use crate::message::{ExecuteCommandRequest, WriteFileRequest};
use std::collections::HashMap;

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum AssemblyFlavor {
    Att,
    Intel,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum DemangleAssembly {
    Demangle,
    Mangle,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum ProcessAssembly {
    Filter,
    Raw,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, strum::IntoStaticStr)]
pub enum CompileTarget {
    Assembly(AssemblyFlavor, DemangleAssembly, ProcessAssembly),
    LlvmIr,
    Mir,
    Hir,
    Wasm,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, strum::IntoStaticStr)]
pub enum Channel {
    Stable,
    Beta,
    Nightly,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, strum::IntoStaticStr)]
pub enum Mode {
    Debug,
    Release,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, strum::IntoStaticStr)]
pub enum Edition {
    Rust2015,
    Rust2018,
    Rust2021, // TODO - add parallel tests for 2021
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, strum::IntoStaticStr)]
pub enum CrateType {
    Binary,
    Library(LibraryType),
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, strum::IntoStaticStr)]
pub enum LibraryType {
    Lib,
    Dylib,
    Rlib,
    Staticlib,
    Cdylib,
    ProcMacro,
}

#[derive(Debug, Clone)]
pub struct CompileRequest {
    pub target: CompileTarget,
    pub channel: Channel,
    pub crate_type: CrateType,
    pub mode: Mode,
    pub edition: Option<Edition>,
    pub tests: bool,
    pub backtrace: bool,
    pub code: String,
}

impl CompileRequest {
    pub(crate) fn write_main_request(&self) -> WriteFileRequest {
        WriteFileRequest {
            path: "src/main.rs".to_owned(),
            content: self.code.clone().into(),
        }
    }

    pub(crate) fn write_cargo_toml_request(&self) -> WriteFileRequest {
        let edition = match self.edition {
            Some(Edition::Rust2021) => "2021",
            Some(Edition::Rust2018) => "2018",
            Some(Edition::Rust2015) => "2015",
            None => "2021",
        };

        WriteFileRequest {
            path: "Cargo.toml".to_owned(),
            content: format!(
                r#"[package]
                   name = "play"
                   version = "0.1.0"
                   edition = "{edition}"
                   "#
            )
            .into(),
        }
    }

    pub(crate) fn execute_cargo_request(&self, output_path: &str) -> ExecuteCommandRequest {
        use CompileTarget::*;

        let mut args = if let Wasm = self.target {
            vec!["wasm", "build"]
        } else {
            vec!["rustc"]
        };
        if let Mode::Release = self.mode {
            args.push("--release");
        }

        match self.target {
            Assembly(flavor, _, _) => {
                use crate::sandbox::AssemblyFlavor::*;

                // TODO: No compile-time string formatting.
                args.extend(&["--", "--emit", "asm=compilation"]);

                // Enable extra assembly comments for nightly builds
                if let Channel::Nightly = self.channel {
                    args.push("-Z");
                    args.push("asm-comments");
                }

                args.push("-C");
                match flavor {
                    Att => args.push("llvm-args=-x86-asm-syntax=att"),
                    Intel => args.push("llvm-args=-x86-asm-syntax=intel"),
                }
            }
            LlvmIr => args.extend(&["--", "--emit", "llvm-ir=compilation"]),
            Mir => args.extend(&["--", "--emit", "mir=compilation"]),
            Hir => args.extend(&["--", "-Zunpretty=hir", "-o", output_path]),
            Wasm => args.extend(&["-o", output_path]),
        }
        let mut envs = HashMap::new();
        if self.backtrace {
            envs.insert("RUST_BACKTRACE".to_owned(), "1".to_owned());
        }

        ExecuteCommandRequest {
            cmd: "cargo".to_owned(),
            args: args.into_iter().map(|s| s.to_owned()).collect(),
            envs,
            cwd: None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct CompileResponse {
    pub success: bool,
    pub code: String,
    pub stdout: String,
    pub stderr: String,
}

#[derive(Debug, Clone)]
pub struct CompileResponse2 {
    pub success: bool,
    pub code: String,
}
