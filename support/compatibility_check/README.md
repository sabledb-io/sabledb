# Redis Compatibility Checker

A Bun.js tool to analyze Redis command compatibility for SableDB.

## Overview

This tool reads all Redis command metadata from the `@commands` folder and generates:
- A comprehensive markdown report with statistics and command details
- A JSON file containing the structured data for programmatic access

## Features

- **Command Parsing**: Reads all JSON files from `@commands` directory
- **Grouping**: Organizes commands by their Redis group (string, hash, list, etc.)
- **Statistics**: Calculates counts and percentages for each group
- **Multiple Output Formats**:
  - Console summary with visual progress bars
  - Detailed markdown report
  - JSON data structure

## Usage

### Prerequisites

Make sure you have [Bun](https://bun.sh) installed.

### Initial Setup

First, generate the support file (only needed once or when new commands are added):

```bash
bun run generate-support
```

This creates `sabledb-support.jsonl` with all commands marked as `supported: false`.

### Marking Commands as Supported

Edit `sabledb-support.jsonl` to update the support status for each command:

```jsonl
{"name":"APPEND","supported":true,"notes":""}
{"name":"BGREWRITEAOF","supported":false,"notes":"AOF not implemented yet"}
{"name":"GET","supported":true,"notes":""}
```

### Running the Tool

```bash
# From the support/compatibility_check directory
bun run generate-compatibility-file.ts

# Or using the npm script
bun run generate-compatibility
```

### Output Files

The tool generates two files:

1. **docs/COMPATIBILITY.md**: A markdown report written to the docs folder containing:
   - Overall compatibility statistics
   - Commands grouped by category with support percentages
   - Detailed tables for each group showing support status and notes

2. **commands-data.json**: A JSON file (in this directory) containing the complete data structure for programmatic access

### Tracked Files

- **sabledb-support.jsonl**: Checked into git - tracks which commands are supported
- **docs/COMPATIBILITY.md**: Generated report checked into git

## Project Structure

```
support/compatibility_check/
├── generate-support-file.ts  # Script to generate initial support file
├── index.ts                  # Main analysis script
├── package.json              # Project configuration
├── tsconfig.json             # TypeScript configuration
├── README.md                 # This file
├── sabledb-support.jsonl     # SableDB support status (checked in)
└── commands-data.json        # Generated JSON data (not checked in)

docs/
└── COMPATIBILITY.md          # Generated markdown report (checked in)
```

## Command Metadata Structure

Each command JSON file in `@commands` contains:

```json
{
  "COMMAND_NAME": {
    "summary": "Brief description",
    "complexity": "Time complexity",
    "group": "Command group (string, hash, etc.)",
    "since": "Redis version",
    "arity": 2,
    "function": "functionName",
    "command_flags": ["FLAG1", "FLAG2"],
    "acl_categories": ["CATEGORY"],
    "arguments": [...]
  }
}
```

## SableDB Support File Format

The `sabledb-support.jsonl` file tracks implementation status separately from the Redis command metadata:

```jsonl
{"name":"APPEND","supported":true,"notes":""}
{"name":"BGREWRITEAOF","supported":false,"notes":"AOF not implemented yet"}
{"name":"GET","supported":true,"notes":"Fully supported"}
```

### Schema

Each line is a JSON object with:
- `name` (string): Command name in uppercase
- `supported` (boolean): Whether the command is implemented in SableDB
- `notes` (string): Optional notes about the implementation or why it's not supported

### Workflow

1. Generate initial file: `bun run generate-support`
2. Manually edit the file to mark supported commands
3. Commit the file to git
4. The main script will merge this data with Redis metadata for reports


## License

Part of the SableDB project.
