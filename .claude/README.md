# Claude Code for the Airbyte Python CDK

This directory contains skills and subagents that extend Claude Code with CDK-specific capabilities.

## Skills

Skills are invoked via slash commands in Claude Code (e.g., `/explain`).

| Skill | Command | Description |
|-------|---------|-------------|
| **Explain** | `/explain <topic>` | Explains how CDK components, architecture, or specific connectors work. Reads local CDK source and can fetch connector code from the Airbyte monorepo. Saves a report to `thoughts/explanations/`. Use `--fast` for a quick inline answer. |
| **Diagram** | `/diagram <topic>` | Generates Mermaid flowcharts and sequence diagrams for CDK code flows. Can diagram a specific concept or the changes on your current branch. Saves output to `thoughts/diagrams/`. |
| **Create PR** | `/create-pr` | Creates a GitHub pull request with a semantic title and auto-generated description. Analyzes the branch diff, generates a structured PR body, and opens the PR via `gh`. Use `--title` to provide a custom title. |
| **Generate PR Description** | `/generate-pr-description` | Generates a PR description from the current branch diff without creating the PR. Useful for previewing before opening. |

## Subagents

Subagents are research-focused agents that Claude Code spawns automatically when it needs specialized knowledge. You don't invoke these directly — Claude uses them behind the scenes during tasks.

| Agent | When it's used |
|-------|---------------|
| **cdk-code-researcher** | When Claude needs to understand CDK internals — pagination, auth, retrievers, requesters, extractors, incremental sync, stream slicing, or the runtime/entrypoint flow. Explores the local CDK source code. |
| **cdk-schema-researcher** | When Claude needs to trace how a manifest YAML component maps through the schema, Pydantic models, and `ModelToComponentFactory` to a runtime Python object. |
| **connector-researcher** | When Claude needs to inspect a specific connector's manifest, metadata, or Python source from the Airbyte monorepo on GitHub. |

## Directory structure

```
.claude/
├── README.md              # This file
├── agents/                # Subagent definitions
│   ├── cdk-code-researcher.md
│   ├── cdk-schema-researcher.md
│   └── connector-researcher.md
└── skills/                # Skill definitions
    ├── create-pr/
    │   └── SKILL.md
    ├── diagram/
    │   └── SKILL.md
    ├── explain/
    │   └── SKILL.md
    └── generate-pr-description/
        └── SKILL.md
```
