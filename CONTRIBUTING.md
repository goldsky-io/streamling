# Contributing to Streamling

## Design

Streamling is a performant and extensible streaming data framework.

It’s designed to be extended via plugins. If you consider making a contribution, such as a new connector or a function, **a plugin extension is the recommended approach**.

Streamling is designed to be executed as **a single-node engine**. Horizontal scaling is possible via Kafka Consumer groups and other mechanisms, but individual Streamling instances are completely **independent**. For example, it means that the progress of each instance is independent. Checkpoints are also handled within the instance; they’re not distributed or coordinated. It is possible this design may change in the future, but it’s not on the roadmap at the moment.

## Development Environment

Streamling is implemented in Rust using standard Rust tooling like Cargo. 

It’s recommended to use [Justfile](https://github.com/casey/just) for local development. E.g. `just build` or `just test`. Run `just` to see the list of available commands. 

### End-to-end Testing

Streamling uses a containerized setup to run end-to-end tests. It relies on a local Kubernetes cluster with K3s and Docker Desktop as supported providers. 

Setup your environment with `just env-setup` and then run tests with `just e2e-test` or `just e2e-test-debug`, for example:

```bash
K8S_PROVIDER=docker-desktop RUST_LOG=debug just e2e-test-debug test_sql_transform_filter
```

## Conventional Commits & Labels

We follow the [Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/) specification to categorize PRs based on the title. This most often simply means looking for titles starting with prefixes such as `fix:`, `feat:`, `docs:`, or `chore:`. We do not enforce this convention, but encourage its use if you want your PR to feature in the correct section of the changelog.

## Pull Request Workflow

Pull requests are welcome from anyone in the community.

The lifecycle of a PR is:

- Create a PR targeting the `main` branch.
- Ensure that all CI jobs (tests, linter, etc.) pass.
- Your PR will be reviewed. Please address all feedback on the PR; you don’t have to change the code, but you should acknowledge it.
- Once the PR is approved, one of the committers will merge it. All commits in the PR are squashed into a single commit when merged.

### Pull Request Guidelines

- A well-written PR description will increase the chances of getting a review sooner.
- When possible, split your contributions into multiple smaller, focused PRs. Large PRs are harder to review.
- PRs with bug fixes should contain tests to prevent regressions.
- AI-assisted contributions are welcome, but you’re fully responsible for the complete PR. See the `AI-Assisted Contributions` section below.

## AI-Assisted Contributions

Streamling has the following policy for AI-assisted PRs:

- The PR author should **understand the core ideas** behind the implementation **end-to-end**, and be able to justify the design and code during review.
- **Calls out unknowns and assumptions**. It’s okay to not fully understand some bits of AI-generated code. You should comment on these cases and point them out to reviewers so that they can use their knowledge of the codebase to clear up any concerns.

### Why Fully AI-Generated PRs Without Understanding Are Not Helpful

Today, AI tools cannot reliably make complex changes to Streamling on their own, which is why we rely on pull requests and code review.

The purposes of code review are:

1. Finish the intended task.
2. Share knowledge between authors and reviewers as a long-term investment in the project. For this reason, even if someone familiar with the codebase can finish a task quickly, we’re still happy to help a new contributor work on it, even if it takes longer.

An AI dump for an issue doesn’t meet these purposes. Maintainers could finish the task faster by using AI directly, and the submitters gain little knowledge if they act only as a pass-through AI proxy without understanding.

Please understand the reviewing capacity is **very limited** for the project, so large PRs which appear not to have the requisite understanding might not get reviewed, and eventually closed.

### Better Ways to Contribute Than an “AI Dump”

It’s recommended to write a high-quality issue with a clear problem statement and a minimal, reproducible example. This can make it easier for others to contribute.

## Contributor License Agreement (CLA)

By contributing to Streamling, you agree to the following terms:

1. **Grant of Rights:** You grant Endless Sky Inc. the rights to use, modify, and distribute your contributions in both open-source and enterprise versions of the software.
2. **Warranty:** You warrant that you have the right to grant these rights and that your contributions do not infringe on any third-party rights.
3. **Acceptance:** This agreement is effective upon your first contribution.
