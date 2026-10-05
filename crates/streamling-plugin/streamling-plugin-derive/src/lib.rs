#![allow(clippy::missing_const_for_thread_local)]

use proc_macro::TokenStream;
use quote::quote;
use syn::{Ident, LitStr, Token, parse::Parse, parse::ParseStream, parse_macro_input};

// Type aliases to keep static/thread_local types readable and clippy-friendly
type ChannelCapTriplet = (u32, u32, u32);
type PluginCapsEntry = (String, ChannelCapTriplet);

thread_local! {
    // Can't store the actual TokenStream2 due to the "use-after-free" compiler error
    static PLUGIN_COMPONENTS: std::cell::RefCell<Vec<String>> = std::cell::RefCell::new(Vec::new());
    static PLUGIN_IDS: std::cell::RefCell<Vec<String>> = std::cell::RefCell::new(Vec::new());
    static PLUGIN_CAPS: std::cell::RefCell<Vec<PluginCapsEntry>> = std::cell::RefCell::new(Vec::new());
    static PLUGIN_PREPROCESSOR_COMPONENTS: std::cell::RefCell<Vec<String>> = std::cell::RefCell::new(Vec::new());
    static PLUGIN_PREPROCESSOR_IDS: std::cell::RefCell<Vec<String>> = std::cell::RefCell::new(Vec::new());
    static PLUGIN_UDF_COMPONENTS: std::cell::RefCell<Vec<String>> = std::cell::RefCell::new(Vec::new());
    static PLUGIN_UDF_DESCRIPTOR_FNS: std::cell::RefCell<Vec<String>> = std::cell::RefCell::new(Vec::new());
    static PLUGIN_SIDE_OUTPUT_COMPONENTS: std::cell::RefCell<Vec<String>> = std::cell::RefCell::new(Vec::new());
    static PLUGIN_SIDE_OUTPUT_DESCRIPTOR_FNS: std::cell::RefCell<Vec<String>> = std::cell::RefCell::new(Vec::new());
}

// Helper function to generate both component_id and plugin_id string
fn generate_plugin_identifiers(
    namespace: &Option<LitStr>,
    name: &LitStr,
) -> (proc_macro2::TokenStream, String) {
    let plugin_id = if let Some(namespace) = namespace {
        format!("{}.{}", namespace.value(), name.value())
    } else {
        name.value()
    };

    // Create a string literal token for the match pattern
    let component_id = quote! { #plugin_id };

    (component_id, plugin_id)
}

struct PluginComponent {
    namespace: Option<LitStr>,
    name: LitStr,
    component_type: Ident,
    caps: Vec<u32>,
}

impl Parse for PluginComponent {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let namespace_or_name: LitStr = input.parse()?;
        input.parse::<Token![,]>()?;
        if input.peek(LitStr) {
            // two literals, so namespace and name
            let name: LitStr = input.parse()?;
            input.parse::<Token![,]>()?;
            let component_type: Ident = input.parse()?;
            // Optional capacities
            let mut caps: Vec<u32> = Vec::new();
            while input.peek(Token![,]) {
                input.parse::<Token![,]>()?;
                if input.is_empty() {
                    break;
                }
                if let Ok(int_lit) = input.parse::<syn::LitInt>() {
                    caps.push(int_lit.base10_parse::<u32>()?);
                } else {
                    // If the next token isn't an int, stop parsing caps
                    break;
                }
            }
            Ok(PluginComponent {
                namespace: Some(namespace_or_name),
                name,
                component_type,
                caps,
            })
        } else if input.peek(Ident) {
            // identifier after a single literal, so no namespace
            let component_type: Ident = input.parse()?;
            // Optional capacities
            let mut caps: Vec<u32> = Vec::new();
            while input.peek(Token![,]) {
                input.parse::<Token![,]>()?;
                if input.is_empty() {
                    break;
                }
                if let Ok(int_lit) = input.parse::<syn::LitInt>() {
                    caps.push(int_lit.base10_parse::<u32>()?);
                } else {
                    break;
                }
            }
            Ok(PluginComponent {
                namespace: None,
                name: namespace_or_name,
                component_type,
                caps,
            })
        } else {
            Err(syn::Error::new_spanned(
                namespace_or_name,
                "Expected a string literal or identifier for the namespace or name",
            ))
        }
    }
}

// Explicit setters for input/output caps as trailing macros
// Usage: set_plugin_input_buffer!("plugin.id", 1);
//        set_plugin_output_buffer!("plugin.id", 2);
#[proc_macro]
pub fn set_plugin_input_buffer(input: TokenStream) -> TokenStream {
    let input2 = proc_macro2::TokenStream::from(input);
    let mut tokens = input2.into_iter();

    // Parse plugin_id (string literal)
    let plugin_id_lit = match tokens.next() {
        Some(proc_macro2::TokenTree::Literal(lit)) => {
            let lit_str = lit.to_string();
            if lit_str.starts_with('"') && lit_str.ends_with('"') {
                lit_str[1..lit_str.len() - 1].to_string()
            } else {
                return syn::Error::new_spanned(
                    lit,
                    "First argument must be a string literal plugin id",
                )
                .to_compile_error()
                .into();
            }
        }
        other => {
            return syn::Error::new_spanned(
                quote::quote! { #other },
                "First argument must be a string literal plugin id",
            )
            .to_compile_error()
            .into();
        }
    };

    // Skip comma
    match tokens.next() {
        Some(proc_macro2::TokenTree::Punct(punct)) if punct.as_char() == ',' => {}
        _ => {
            return syn::Error::new_spanned(
                quote::quote! { set_plugin_input_buffer },
                "Expected comma after plugin id",
            )
            .to_compile_error()
            .into();
        }
    }

    // Parse capacity (integer literal)
    let in_cap = match tokens.next() {
        Some(proc_macro2::TokenTree::Literal(lit)) => lit.to_string().parse::<u32>().unwrap_or(1),
        other => {
            return syn::Error::new_spanned(
                quote::quote! { #other },
                "Second argument must be an integer capacity",
            )
            .to_compile_error()
            .into();
        }
    };

    PLUGIN_CAPS.with(|caps| caps.borrow_mut().push((plugin_id_lit, (in_cap, 0, 0))));
    TokenStream::new()
}

#[proc_macro]
pub fn set_plugin_output_buffer(input: TokenStream) -> TokenStream {
    let input2 = proc_macro2::TokenStream::from(input);
    let mut tokens = input2.into_iter();

    // Parse plugin_id (string literal)
    let plugin_id_lit = match tokens.next() {
        Some(proc_macro2::TokenTree::Literal(lit)) => {
            let lit_str = lit.to_string();
            if lit_str.starts_with('"') && lit_str.ends_with('"') {
                lit_str[1..lit_str.len() - 1].to_string()
            } else {
                return syn::Error::new_spanned(
                    lit,
                    "First argument must be a string literal plugin id",
                )
                .to_compile_error()
                .into();
            }
        }
        other => {
            return syn::Error::new_spanned(
                quote::quote! { #other },
                "First argument must be a string literal plugin id",
            )
            .to_compile_error()
            .into();
        }
    };

    // Skip comma
    match tokens.next() {
        Some(proc_macro2::TokenTree::Punct(punct)) if punct.as_char() == ',' => {}
        _ => {
            return syn::Error::new_spanned(
                quote::quote! { set_plugin_output_buffer },
                "Expected comma after plugin id",
            )
            .to_compile_error()
            .into();
        }
    }

    // Parse capacity (integer literal)
    let out_cap = match tokens.next() {
        Some(proc_macro2::TokenTree::Literal(lit)) => lit.to_string().parse::<u32>().unwrap_or(1),
        other => {
            return syn::Error::new_spanned(
                quote::quote! { #other },
                "Second argument must be an integer capacity",
            )
            .to_compile_error()
            .into();
        }
    };

    PLUGIN_CAPS.with(|caps| caps.borrow_mut().push((plugin_id_lit, (0, out_cap, 0))));
    TokenStream::new()
}

#[allow(dead_code)]
struct InitPlugin;

impl Parse for InitPlugin {
    fn parse(_input: ParseStream) -> syn::Result<Self> {
        Ok(InitPlugin)
    }
}

#[proc_macro]
pub fn register_plugin_source(input: TokenStream) -> TokenStream {
    let PluginComponent {
        namespace,
        name,
        component_type,
        caps: _caps,
    } = parse_macro_input!(input as PluginComponent);

    let (component_id, plugin_id) = generate_plugin_identifiers(&namespace, &name);

    let component = quote! {
        #component_id => source_generator(
            plugin_id,
            |rt, state, metrics, opts| streamling_plugin::IntoSourcePluginResult::into_source_result(#component_type::new(rt, state, metrics, opts)),
            options,
            runtime,
            state_backend_config,
            message_channels,
        ),
    };

    PLUGIN_COMPONENTS.with(|components| components.borrow_mut().push(component.to_string()));
    PLUGIN_IDS.with(|ids| ids.borrow_mut().push(plugin_id));

    TokenStream::new()
}

#[proc_macro]
pub fn register_plugin_transform(input: TokenStream) -> TokenStream {
    let PluginComponent {
        namespace,
        name,
        component_type,
        caps: _caps,
    } = parse_macro_input!(input as PluginComponent);

    let (component_id, plugin_id) = generate_plugin_identifiers(&namespace, &name);

    let component = quote! {
        #component_id => transform_generator(
            plugin_id,
            |schema, rt, state, metrics, opts| streamling_plugin::IntoTransformPluginResult::into_transform_result(#component_type::new(schema, rt, state, metrics, opts)),
            input_schema.expect("Input schema must be defined for transforms"),
            options,
            runtime,
            state_backend_config,
            message_channels,
        ),
    };

    PLUGIN_COMPONENTS.with(|components| components.borrow_mut().push(component.to_string()));
    PLUGIN_IDS.with(|ids| ids.borrow_mut().push(plugin_id));

    TokenStream::new()
}

#[proc_macro]
pub fn register_plugin_sink(input: TokenStream) -> TokenStream {
    let PluginComponent {
        namespace,
        name,
        component_type,
        caps: _caps,
    } = parse_macro_input!(input as PluginComponent);

    let (component_id, plugin_id) = generate_plugin_identifiers(&namespace, &name);

    let component = quote! {
        #component_id => sink_generator(
            plugin_id,
            |schema, rt, state, metrics, opts| streamling_plugin::IntoSinkPluginResult::into_sink_result(#component_type::new(schema, rt, state, metrics, opts)),
            input_schema.expect("Input schema must be defined for sinks"),
            options,
            runtime,
            state_backend_config,
            message_channels,
        ),
    };

    PLUGIN_COMPONENTS.with(|components| components.borrow_mut().push(component.to_string()));
    PLUGIN_IDS.with(|ids| ids.borrow_mut().push(plugin_id));

    TokenStream::new()
}

#[proc_macro]
pub fn register_plugin_preprocessor(input: TokenStream) -> TokenStream {
    let PluginComponent {
        namespace,
        name,
        component_type,
        caps: _caps,
    } = parse_macro_input!(input as PluginComponent);

    let (component_id, plugin_id) = generate_plugin_identifiers(&namespace, &name);

    let component = quote! {
        #component_id => preprocessor_generator(
            plugin_id,
            |opts| #component_type::new(opts).map(|p| std::sync::Arc::new(p) as std::sync::Arc<dyn streamling_plugin::PreprocessorPlugin>),
            options,
            runtime,
            message_channels,
        ),
    };

    PLUGIN_PREPROCESSOR_COMPONENTS
        .with(|components| components.borrow_mut().push(component.to_string()));
    PLUGIN_PREPROCESSOR_IDS.with(|ids| ids.borrow_mut().push(plugin_id));

    TokenStream::new()
}

/// Registers a factory function that returns a `ScalarUDF` as a plugin UDF.
///
/// Usage: `register_plugin_udf_fn!(create_my_udf);`
///
/// The function must have signature `fn() -> ScalarUDF`.
#[proc_macro]
pub fn register_plugin_udf_fn(input: TokenStream) -> TokenStream {
    let factory_fn: Ident = parse_macro_input!(input as Ident);

    let static_name = Ident::new(
        &format!("PLUGIN_UDF_FN_{}", factory_fn.to_string().to_uppercase()),
        factory_fn.span(),
    );
    let invoke_fn_name = Ident::new(
        &format!(
            "plugin_udf_fn_invoke_{}",
            factory_fn.to_string().to_lowercase()
        ),
        factory_fn.span(),
    );
    let descriptor_fn_name = Ident::new(
        &format!(
            "plugin_udf_fn_descriptor_{}",
            factory_fn.to_string().to_lowercase()
        ),
        factory_fn.span(),
    );

    let component = quote! {
        static #static_name: std::sync::OnceLock<datafusion::logical_expr::ScalarUDF> = std::sync::OnceLock::new();

        extern "C" fn #invoke_fn_name(
            args: RVec<streamling_plugin::SafeUdfArg>,
            number_rows: usize,
        ) -> RResult<streamling_plugin::SafeArrowColumn, RString> {
            let udf = #static_name.get_or_init(#factory_fn);
            streamling_plugin::invoke_plugin_udf(udf.inner().as_ref(), args, number_rows)
        }

        fn #descriptor_fn_name() -> Result<streamling_plugin::PluginUdfDescriptor, PluginInitializationError> {
            let udf = #static_name.get_or_init(#factory_fn);
            streamling_plugin::build_plugin_udf_descriptor(udf.inner().as_ref(), #invoke_fn_name)
        }
    };

    PLUGIN_UDF_COMPONENTS.with(|components| {
        components.borrow_mut().push(component.to_string());
    });
    PLUGIN_UDF_DESCRIPTOR_FNS.with(|fns| {
        fns.borrow_mut().push(descriptor_fn_name.to_string());
    });

    TokenStream::new()
}

/// Registers a type implementing `ScalarUDFImpl` as a plugin UDF.
///
/// Usage: `register_plugin_udf!(MyUdfType);`
///
/// The type must implement `ScalarUDFImpl` and `Default`.
#[proc_macro]
pub fn register_plugin_udf(input: TokenStream) -> TokenStream {
    let udf_type: Ident = parse_macro_input!(input as Ident);

    let static_name = Ident::new(
        &format!("PLUGIN_UDF_{}", udf_type.to_string().to_uppercase()),
        udf_type.span(),
    );
    let invoke_fn_name = Ident::new(
        &format!("plugin_udf_invoke_{}", udf_type.to_string().to_lowercase()),
        udf_type.span(),
    );
    let descriptor_fn_name = Ident::new(
        &format!(
            "plugin_udf_descriptor_{}",
            udf_type.to_string().to_lowercase()
        ),
        udf_type.span(),
    );

    let component = quote! {
        static #static_name: std::sync::OnceLock<#udf_type> = std::sync::OnceLock::new();

        extern "C" fn #invoke_fn_name(
            args: RVec<streamling_plugin::SafeUdfArg>,
            number_rows: usize,
        ) -> RResult<streamling_plugin::SafeArrowColumn, RString> {
            let instance = #static_name.get_or_init(|| #udf_type::new());
            streamling_plugin::invoke_plugin_udf(instance, args, number_rows)
        }

        fn #descriptor_fn_name() -> Result<streamling_plugin::PluginUdfDescriptor, PluginInitializationError> {
            let instance = #static_name.get_or_init(|| #udf_type::new());
            streamling_plugin::build_plugin_udf_descriptor(instance, #invoke_fn_name)
        }
    };

    PLUGIN_UDF_COMPONENTS.with(|components| {
        components.borrow_mut().push(component.to_string());
    });
    PLUGIN_UDF_DESCRIPTOR_FNS.with(|fns| {
        fns.borrow_mut().push(descriptor_fn_name.to_string());
    });

    TokenStream::new()
}

/// Registers a type implementing `SideOutputPlugin` as a plugin side output.
///
/// Usage: `register_plugin_side_output!(MySideOutputType);`
/// Or with explicit id: `register_plugin_side_output!("my_id", MySideOutputType);`
///
/// The type must implement `SideOutputPlugin`.
#[proc_macro]
pub fn register_plugin_side_output(input: TokenStream) -> TokenStream {
    // Parse: either just an Ident, or a LitStr "," Ident
    let input2: proc_macro2::TokenStream = input.into();
    let mut iter = input2.into_iter().peekable();

    // Check if first token is a string literal (explicit id)
    let (side_output_id, side_output_type) = match iter.peek() {
        Some(proc_macro2::TokenTree::Literal(_)) => {
            let lit = match iter.next().unwrap() {
                proc_macro2::TokenTree::Literal(lit) => {
                    let s = lit.to_string();
                    if s.starts_with('"') && s.ends_with('"') {
                        s[1..s.len() - 1].to_string()
                    } else {
                        return syn::Error::new_spanned(lit, "Expected string literal for id")
                            .to_compile_error()
                            .into();
                    }
                }
                _ => unreachable!(),
            };
            // Skip comma
            iter.next();
            let type_ident: proc_macro2::TokenStream = iter.collect();
            let type_ident: Ident =
                syn::parse2(type_ident).expect("Expected type identifier after id");
            (lit, type_ident)
        }
        _ => {
            let type_tokens: proc_macro2::TokenStream = iter.collect();
            let type_ident: Ident = syn::parse2(type_tokens).expect("Expected type identifier");
            let id = type_ident.to_string().to_lowercase();
            (id, type_ident)
        }
    };

    let static_name = Ident::new(
        &format!(
            "PLUGIN_SIDE_OUTPUT_{}",
            side_output_type.to_string().to_uppercase()
        ),
        side_output_type.span(),
    );
    let init_fn_name = Ident::new(
        &format!(
            "plugin_side_output_initialize_{}",
            side_output_type.to_string().to_lowercase()
        ),
        side_output_type.span(),
    );
    let process_fn_name = Ident::new(
        &format!(
            "plugin_side_output_process_batch_{}",
            side_output_type.to_string().to_lowercase()
        ),
        side_output_type.span(),
    );
    let shutdown_fn_name = Ident::new(
        &format!(
            "plugin_side_output_shutdown_{}",
            side_output_type.to_string().to_lowercase()
        ),
        side_output_type.span(),
    );
    let descriptor_fn_name = Ident::new(
        &format!(
            "plugin_side_output_descriptor_{}",
            side_output_type.to_string().to_lowercase()
        ),
        side_output_type.span(),
    );

    let component = quote! {
        static #static_name: std::sync::LazyLock<
            std::sync::RwLock<std::collections::HashMap<String, #side_output_type>>
        > = std::sync::LazyLock::new(|| std::sync::RwLock::new(std::collections::HashMap::new()));

        extern "C" fn #init_fn_name(
            source_name: RString,
            schema: streamling_plugin::SafeArrowSchema,
            options: streamling_plugin::PluginOptions,
            metrics_recorder: streamling_plugin::ffi::PluginMetricsRecorder,
        ) -> RResult<(), RString> {
            let schema_ref: arrow::datatypes::SchemaRef = schema.into();
            let instance = #side_output_type::new(
                source_name.as_str(),
                schema_ref,
                options.as_rust(),
                metrics_recorder,
            );
            let mut map = #static_name.write().expect("side output instance map write lock");
            map.insert(source_name.as_str().to_string(), instance);
            RResult::ROk(())
        }

        extern "C" fn #process_fn_name(
            source_name: RString,
            data: streamling_plugin::ffi::SafeArrowArray,
        ) -> RResult<(), RString> {
            let map = #static_name.read().expect("side output instance map read lock");
            let instance = match map.get(source_name.as_str()) {
                Some(i) => i,
                None => return RResult::RErr(RString::from("Side output not initialized for source")),
            };
            let batch: arrow::array::RecordBatch = data.into();
            match instance.process_batch(&batch) {
                Ok(()) => RResult::ROk(()),
                Err(msg) => RResult::RErr(RString::from(msg)),
            }
        }

        extern "C" fn #shutdown_fn_name() -> RResult<(), RString> {
            let mut map = #static_name.write().expect("side output instance map write lock");
            for instance in map.values() {
                instance.shutdown();
            }
            // Clear the map to drop all instances, which drops their PluginMetricsRecorders
            // and disconnects the metrics channels so process_plugin_metrics tasks can exit.
            map.clear();
            RResult::ROk(())
        }

        fn #descriptor_fn_name() -> streamling_plugin::PluginSideOutputDescriptor {
            streamling_plugin::PluginSideOutputDescriptor {
                id: RString::from(#side_output_id),
                initialize: #init_fn_name,
                process_batch: #process_fn_name,
                shutdown: #shutdown_fn_name,
            }
        }
    };

    PLUGIN_SIDE_OUTPUT_COMPONENTS.with(|components| {
        components.borrow_mut().push(component.to_string());
    });
    PLUGIN_SIDE_OUTPUT_DESCRIPTOR_FNS.with(|fns| {
        fns.borrow_mut().push(descriptor_fn_name.to_string());
    });

    TokenStream::new()
}

#[proc_macro]
pub fn init_plugin(_input: TokenStream) -> TokenStream {
    generate_init_plugin_code(false)
}

#[proc_macro]
pub fn init_plugin_with_async_runtime(_input: TokenStream) -> TokenStream {
    generate_init_plugin_code(true)
}

fn generate_init_plugin_code(use_direct_tokio: bool) -> TokenStream {
    let components = PLUGIN_COMPONENTS.with(|components| {
        let borrowed = components.borrow();
        let mut combined = proc_macro2::TokenStream::new();

        for component_str in borrowed.iter() {
            if let Ok(component_tokens) = component_str.parse::<proc_macro2::TokenStream>() {
                combined.extend(component_tokens);
            }
        }

        combined
    });

    let preprocessor_components = PLUGIN_PREPROCESSOR_COMPONENTS.with(|components| {
        let borrowed = components.borrow();
        let mut combined = proc_macro2::TokenStream::new();

        for component_str in borrowed.iter() {
            if let Ok(component_tokens) = component_str.parse::<proc_macro2::TokenStream>() {
                combined.extend(component_tokens);
            }
        }

        combined
    });

    let udf_components = PLUGIN_UDF_COMPONENTS.with(|components| {
        let borrowed = components.borrow();
        let mut combined = proc_macro2::TokenStream::new();

        for component_str in borrowed.iter() {
            if let Ok(component_tokens) = component_str.parse::<proc_macro2::TokenStream>() {
                combined.extend(component_tokens);
            }
        }

        combined
    });

    let side_output_components = PLUGIN_SIDE_OUTPUT_COMPONENTS.with(|components| {
        let borrowed = components.borrow();
        let mut combined = proc_macro2::TokenStream::new();

        for component_str in borrowed.iter() {
            if let Ok(component_tokens) = component_str.parse::<proc_macro2::TokenStream>() {
                combined.extend(component_tokens);
            }
        }

        combined
    });

    let udf_descriptor_calls: Vec<proc_macro2::TokenStream> =
        PLUGIN_UDF_DESCRIPTOR_FNS.with(|fns| {
            fns.borrow()
                .iter()
                .map(|fn_name| {
                    let ident: proc_macro2::TokenStream = fn_name.parse().unwrap();
                    quote! { #ident() }
                })
                .collect()
        });

    let side_output_descriptor_calls: Vec<proc_macro2::TokenStream> =
        PLUGIN_SIDE_OUTPUT_DESCRIPTOR_FNS.with(|fns| {
            fns.borrow()
                .iter()
                .map(|fn_name| {
                    let ident: proc_macro2::TokenStream = fn_name.parse().unwrap();
                    quote! { #ident() }
                })
                .collect()
        });

    let has_udfs = !udf_descriptor_calls.is_empty();
    let has_side_outputs = !side_output_descriptor_calls.is_empty();

    let plugin_ids = PLUGIN_IDS.with(|ids| ids.borrow().clone());
    let preprocessor_ids = PLUGIN_PREPROCESSOR_IDS.with(|ids| ids.borrow().clone());
    let all_plugin_ids: Vec<String> = plugin_ids
        .iter()
        .chain(preprocessor_ids.iter())
        .cloned()
        .collect();

    let plugin_caps = PLUGIN_CAPS.with(|caps| caps.borrow().clone());
    let caps_inits: Vec<proc_macro2::TokenStream> = plugin_caps
        .iter()
        .map(|(id, (a, b, c))| {
            quote! {
                default_channel_caps.insert(RString::from(#id), PluginChannelCaps { input: #a, output: #b, metrics: #c });
            }
        })
        .collect();

    let async_imports = if use_direct_tokio {
        quote! { use streamling_plugin::r#async::{PluginAsyncRuntimeObj, DirectTokioProxy}; }
    } else {
        quote! { use streamling_plugin::r#async::PluginAsyncRuntimeObj; }
    };

    let (runtime_param, runtime_setup) = if use_direct_tokio {
        (
            quote! { _runtime: PluginAsyncRuntimeObj, },
            quote! { let runtime = DirectTokioProxy::new().into_async_runtime_obj(); },
        )
    } else {
        (quote! { runtime: PluginAsyncRuntimeObj, }, quote! {})
    };

    let udf_descriptors_fn = if has_udfs {
        quote! {
            extern "C" fn udf_descriptors() -> RResult<RVec<streamling_plugin::PluginUdfDescriptor>, PluginInitializationError> {
                let results: Vec<Result<streamling_plugin::PluginUdfDescriptor, PluginInitializationError>> = vec![
                    #(#udf_descriptor_calls),*
                ];
                let mut descriptors = Vec::with_capacity(results.len());
                for result in results {
                    match result {
                        Ok(descriptor) => descriptors.push(descriptor),
                        Err(e) => return RResult::RErr(e),
                    }
                }
                RResult::ROk(descriptors.into())
            }
        }
    } else {
        quote! {
            extern "C" fn udf_descriptors() -> RResult<RVec<streamling_plugin::PluginUdfDescriptor>, PluginInitializationError> {
                RResult::ROk(RVec::new())
            }
        }
    };

    let side_output_descriptors_fn = if has_side_outputs {
        quote! {
            extern "C" fn side_output_descriptors() -> RResult<RVec<streamling_plugin::PluginSideOutputDescriptor>, PluginInitializationError> {
                let descriptors: Vec<streamling_plugin::PluginSideOutputDescriptor> = vec![
                    #(#side_output_descriptor_calls),*
                ];
                RResult::ROk(descriptors.into())
            }
        }
    } else {
        quote! {
            extern "C" fn side_output_descriptors() -> RResult<RVec<streamling_plugin::PluginSideOutputDescriptor>, PluginInitializationError> {
                RResult::ROk(RVec::new())
            }
        }
    };

    let output = quote! {
        use abi_stable::export_root_module;
        use abi_stable::prefix_type::PrefixTypeTrait;
        use abi_stable::std_types::{ROption, RResult, RString, RVec, RHashMap};
        use abi_stable::traits::IntoReprC;
        use streamling_plugin::ffi::SafeArrowSchema;
        #async_imports
        use streamling_plugin::{PluginStateBackendConfig, PluginChannels, PluginInitializationError, PluginLogging, PluginModule, PluginModuleRef, PluginOptions, PluginResult, PluginRuntimeConfiguration, PluginChannelCaps, SideOutputPlugin, sink_generator, source_generator, transform_generator, preprocessor_generator};

        #udf_components
        #side_output_components

        extern "C" fn init(
            logging: PluginLogging,
        ) -> RResult<PluginRuntimeConfiguration, PluginInitializationError> {
            logging.initialize_logging();

            let plugin_ids: RVec<RString> = vec![
                #(#all_plugin_ids.to_string().into_c()),*
            ].into();

            let mut default_channel_caps: RHashMap<RString, PluginChannelCaps> = RHashMap::new();
            #(#caps_inits)*

            Ok(PluginRuntimeConfiguration {
                plugin_ids,
                default_channel_caps,
            }).into_c()
        }

        extern "C" fn create(
            plugin_id: RString,
            input_schema: ROption<SafeArrowSchema>,
            options: PluginOptions,
            #runtime_param
            state_backend_config: PluginStateBackendConfig,
            message_channels: PluginChannels,
        ) -> RResult<PluginResult, PluginInitializationError> {
            #runtime_setup

            match plugin_id.as_str() {
                #components
                #preprocessor_components
                _ => Err(PluginInitializationError::NotImplemented).into_c(),
            }
        }

        #udf_descriptors_fn
        #side_output_descriptors_fn

        extern "C" fn set_shutdown_signal(signal: streamling_plugin::shutdown::ShutdownSignalObj) {
            streamling_plugin::shutdown::install_shutdown_signal(signal);
        }

        #[export_root_module]
        pub fn get_module() -> PluginModuleRef {
            PluginModule { init, create, udf_descriptors, side_output_descriptors, set_shutdown_signal }.leak_into_prefix()
        }
    };

    output.into()
}
