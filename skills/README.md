# Streamling Plugin Authoring Skills

A pack of [Agent Skills](https://agentskills.io/) that teach an AI coding agent how to build
Streamling plugins correctly. Install them once and your agent gains the exact registration
macro signatures, constructor contracts, checkpoint/exactly-once lifecycle, error types, and
option/secret patterns — grounded in this repo's plugin API.

## Skills

| Skill | Use when |
|---|---|
| `streamling-plugin-basics` | Creating any plugin crate — cdylib setup, registration macros, constructor contracts, lifecycle, async runtime, errors, options/secrets, metrics, state. **Start here.** |
| `streamling-source-plugin` | Implementing a `SourcePlugin` — output schema + `_gs_op`, `generate_batch`, fetcher/buffer/runner split, resumable cursor pagination, backpressure. |
| `streamling-sink-plugin` | Implementing a `SinkPlugin` — lazy client init, empty-batch guard, NDJSON, batched writes with retry + partial-failure handling, checkpoint acks. |
| `streamling-transform-plugin` | Implementing a `TransformPlugin` — `process_batch`, `output_schema`, arrow-compute filtering. |
| `streamling-udf-plugin` | Implementing a DataFusion scalar UDF — `ScalarUDFImpl`, the modern `invoke_with_args`/`ScalarFunctionArgs` API, calling custom functions from SQL. |
| `streamling-advanced-plugins` | Preprocessors (YAML rewrite), side outputs, multi-kind crate registration, low-level manual FFI. |

The skills cross-reference each other by `skill://<name>` links, which resolve once installed.

## Installation

Each skill is a plain `<name>/SKILL.md` file. Install by symlinking (recommended — stays in sync with the repo) or copying into your coding agent's personal skills directory. **Skills are discovered at agent startup — restart your session after installing.**

### Per-agent skill directories

| Agent | User skills directory |
|---|---|
| Claude Code | `~/.claude/skills/` |
| OpenAI Codex | `~/.codex/skills/`, or `~/.agents/skills/` |
| GitHub Copilot CLI | `~/.copilot/skills/`, or `~/.agents/skills/` |
| Google Gemini CLI | `~/.gemini/skills/`, or `~/.agents/skills/` |
| Antigravity (`agy`) | symlink into `~/.agents/skills/` (or a package `skills/` dir); load each via `view_file … IsSkillFile: true` |

`~/.agents/skills/` is a **cross-runtime alias** shared by Codex, Copilot CLI, and Gemini CLI — installing there covers all three at once. Claude Code reads only `~/.claude/skills/`.

### Recommended — symlink into both common directories

Covers every agent above in one shot:

```bash
cd /path/to/streamling
for d in skills/streamling-*; do
  for dir in ~/.claude/skills ~/.agents/skills; do
    mkdir -p "$dir" && ln -sf "$(pwd)/$d" "$dir/$(basename "$d")"
  done
done
```

### Copy (freeze a snapshot, or runtimes that don't follow symlinks)

```bash
cp -r /path/to/streamling/skills/streamling-* ~/.claude/skills/   # or ~/.agents/skills/
```

## Verifying

After restarting your agent, ask it to list available skills, or check that a skill resolves, e.g. `skill://streamling-plugin-basics`.

## Editing

Edit any `streamling-*/SKILL.md` here and the change takes effect on the next agent restart
(with Option 1/2; with Option 3, re-copy). Keep each skill's `name` and `description` frontmatter
in sync — the description is what the agent uses to decide when to load the skill.
