use serde::{Deserialize, Serialize};
use std::fmt::Write;
use std::path::PathBuf;
#[derive(Debug, Serialize, Deserialize)]
pub struct Metadata {
    pub target_directory: PathBuf,
    pub workspace_root: PathBuf,
    pub packages: Vec<Package>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Package {
    pub manifest_path: PathBuf,
    pub name: String,
    pub targets: Vec<Target>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Target {
    pub name: String,
    pub kind: Vec<Kind>,
    pub src_path: PathBuf,
}

#[derive(Debug, Copy, Clone, Serialize, Deserialize, PartialEq)]
pub enum Kind {
    #[serde(rename = "bin")]
    Bin,
    #[serde(rename = "example")]
    Example,
    #[serde(rename = "test")]
    Test,
    #[serde(rename = "bench")]
    Bench,
    #[serde(rename = "lib")]
    Lib,
    #[serde(rename = "proc-macro")]
    ProcMacro,
    #[serde(rename = "staticlib")]
    StaticLib,
}

impl Metadata {
    pub fn read(pkg: &Option<String>) -> Metadata {
        let output = std::process::Command::new("cargo")
            .arg("metadata")
            .arg("--no-deps")
            .output()
            .expect("Failed to run cargo metadata");
        if !output.status.success() {
            panic!("`cargo metadata` did not succed. Are you inside a cargo crate?");
        }
        let stdout = String::from_utf8(output.stdout).unwrap();
        let mut metadata: Metadata =
            serde_json::from_str(&stdout).expect("Failed to deserialize to Metadata");

        if let Some(pkg_name) = pkg {
            let filtered_packages: Vec<Package> = metadata
                .packages
                .into_iter()
                .filter(|package| &package.name == pkg_name)
                .collect();
            metadata.packages = filtered_packages;
        }

        // We should get exactly 1 package
        if metadata.packages.len() > 1 {
            let mut message = String::from("Found multiple packages in the manifest:\n");
            for package in &metadata.packages {
                writeln!(&mut message, " - {}", package.name).unwrap();
            }
            writeln!(
                &mut message,
                "Use --pkg=<name> to tell me which package to use among the above packages"
            )
            .unwrap();
            panic!("{}", message);
        }

        // Make sure every target is of one kind. Don't know
        // cases where multiple `kind` is possible. So bail out.
        for package in &metadata.packages {
            for target in &package.targets {
                assert!(
                    target.kind.len() == 1,
                    "Found multiple `kind` entries in target {}",
                    target.name
                );
            }
        }
        metadata
    }
}
