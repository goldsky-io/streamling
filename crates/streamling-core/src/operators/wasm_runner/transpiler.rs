use crate::error::Result;
use crate::streamling_err;
use std::sync::Arc;
use swc::{
    Compiler,
    config::{JscConfig, Options},
};
use swc_common::{
    FileName, GLOBALS, SourceMap,
    errors::{ColorConfig, Handler},
};
use swc_ecma_ast::EsVersion;
use swc_ecma_lexer::{Syntax, TsSyntax};

/// Transpiler for TypeScript to JavaScript using SWC.
pub(crate) struct TsToJSTranspiler {
    compiler: Compiler,
    code_map: Arc<SourceMap>,
    options: Options,
}

impl TsToJSTranspiler {
    pub fn new() -> Self {
        let code_map: Arc<SourceMap> = Default::default();
        let compiler = Compiler::new(code_map.clone());
        let mut options = Options::default();

        options.config.jsc = JscConfig {
            target: Some(EsVersion::Es2020), // what extism js-pdk supports
            syntax: Some(Syntax::Typescript(TsSyntax {
                tsx: false,
                decorators: false, // should it be enabled?
                ..Default::default()
            })),
            ..Default::default()
        };

        TsToJSTranspiler {
            compiler,
            code_map,
            options,
        }
    }

    /// Converts supplied TypeScript code to JavaScript.
    pub fn transpile(&self, ts_code: &str) -> Result<String> {
        let handler =
            Handler::with_tty_emitter(ColorConfig::Auto, true, false, Some(self.code_map.clone()));
        let source = self.code_map.new_source_file(
            Arc::new(FileName::Custom("inline.ts".into())),
            ts_code.to_string(),
        );

        // SWC requires its thread‑local "globals" to be set while it runs.
        GLOBALS.set(&Default::default(), || {
            let out = self
                .compiler
                .process_js_file(source, &handler, &self.options)
                .map_err(|e| streamling_err!("TypeScript transpilation failed: {:?}", e))?;
            Ok(out.code)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use indoc::indoc;

    #[test]
    fn test_transpile() {
        let ts_code = r#"
            const x: number = 42;
            const y: string = "Hello, world!";

            type Result = "pass" | "fail"

            function verify(result: Result) {
              if (result === "pass") {
                console.log("Passed")
              } else {
                console.log("Failed")
              }
            }
        "#;

        let transpiler = TsToJSTranspiler::new();
        let js_code = transpiler.transpile(ts_code).unwrap();

        assert_eq!(
            js_code,
            indoc! {r#"
            const x = 42;
            const y = "Hello, world!";
            function verify(result) {
                if (result === "pass") {
                    console.log("Passed");
                } else {
                    console.log("Failed");
                }
            }
        "#}
        );
    }
}
