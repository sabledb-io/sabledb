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

### Running the Tool

```bash
# From the support/compatibility_check directory
bun run index.ts

# Or using the npm script
bun run check
```

### Output Files

The tool generates two files:

1. **COMPATIBILITY.md**: A markdown report containing:
   - Total command count
   - Commands grouped by category with statistics
   - Detailed tables for each group showing command name, summary, version, and complexity

2. **commands-data.json**: A JSON file containing the complete data structure for programmatic access

## Project Structure

```
support/compatibility_check/
├── index.ts              # Main script
├── package.json          # Project configuration
├── README.md             # This file
├── COMPATIBILITY.md      # Generated markdown report
└── commands-data.json    # Generated JSON data
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

## Future Enhancements

### SableDB Implementation Status

The next step is to add SableDB-specific metadata to track which commands are implemented:

```json
{
  "COMMAND_NAME": {
    // ... existing Redis metadata ...
    "sabledb_implemented": true,
    "sabledb_notes": "Optional implementation notes"
  }
}
```

This will enable compatibility tracking and generate reports showing:
- Percentage of implemented commands
- Implementation status by group
- List of missing commands
- Implementation roadmap

## Development

```bash
# Watch mode for development
bun run dev
```

## License

Part of the SableDB project.
