#!/usr/bin/env bun

import { readdirSync, readFileSync } from 'fs';
import { join } from 'path';

// Type definitions for Redis command metadata
interface CommandMetadata {
  summary?: string;
  complexity?: string;
  group?: string;
  since?: string;
  arity?: number;
  function?: string;
  command_flags?: string[];
  acl_categories?: string[];
  history?: Array<[string, string]>;
  arguments?: Array<{
    name: string;
    type: string;
    optional?: boolean;
  }>;
  // SableDB specific metadata (to be added later)
  sabledb_implemented?: boolean;
  sabledb_notes?: string;
}

interface CommandFile {
  [commandName: string]: CommandMetadata;
}

interface GroupedCommands {
  [group: string]: {
    commands: Array<{
      name: string;
      metadata: CommandMetadata;
    }>;
    count: number;
  };
}

interface CommandStats {
  totalCommands: number;
  groups: GroupedCommands;
  groupStats: Array<{
    group: string;
    count: number;
    percentage: number;
  }>;
}

/**
 * Reads all JSON files from the @commands directory
 */
function readCommandFiles(commandsDir: string): Map<string, CommandMetadata> {
  const commands = new Map<string, CommandMetadata>();

  const files = readdirSync(commandsDir).filter(f => f.endsWith('.json'));

  console.log(`Found ${files.length} JSON files in ${commandsDir}`);

  for (const file of files) {
    try {
      const filePath = join(commandsDir, file);
      const content = readFileSync(filePath, 'utf-8');
      const commandFile: CommandFile = JSON.parse(content);

      // Each file contains one command with the command name as the key
      for (const [commandName, metadata] of Object.entries(commandFile)) {
        commands.set(commandName, metadata);
      }
    } catch (error) {
      console.error(`Error reading file ${file}:`, error);
    }
  }

  return commands;
}

/**
 * Groups commands by their group/category
 */
function groupCommandsByType(commands: Map<string, CommandMetadata>): GroupedCommands {
  const grouped: GroupedCommands = {};

  for (const [commandName, metadata] of commands.entries()) {
    const group = metadata.group || 'unknown';

    if (!grouped[group]) {
      grouped[group] = {
        commands: [],
        count: 0
      };
    }

    grouped[group].commands.push({
      name: commandName,
      metadata
    });
    grouped[group].count++;
  }

  // Sort commands within each group alphabetically
  for (const group of Object.keys(grouped)) {
    grouped[group].commands.sort((a, b) => a.name.localeCompare(b.name));
  }

  return grouped;
}

/**
 * Generates statistics about the commands
 */
function generateStats(grouped: GroupedCommands): CommandStats {
  const totalCommands = Object.values(grouped).reduce((sum, g) => sum + g.count, 0);

  const groupStats = Object.entries(grouped)
    .map(([group, data]) => ({
      group,
      count: data.count,
      percentage: (data.count / totalCommands) * 100
    }))
    .sort((a, b) => b.count - a.count);

  return {
    totalCommands,
    groups: grouped,
    groupStats
  };
}

/**
 * Renders the data as a markdown table
 */
function renderMarkdown(stats: CommandStats): string {
  let markdown = '# Redis Command Compatibility Overview\n\n';

  markdown += `**Total Commands:** ${stats.totalCommands}\n\n`;

  // Group statistics table
  markdown += '## Commands by Group\n\n';
  markdown += '| Group | Count | Percentage |\n';
  markdown += '|-------|-------|------------|\n';

  for (const { group, count, percentage } of stats.groupStats) {
    markdown += `| ${group} | ${count} | ${percentage.toFixed(2)}% |\n`;
  }

  markdown += '\n';

  // Detailed breakdown by group
  markdown += '## Detailed Command List\n\n';

  const sortedGroups = stats.groupStats.map(s => s.group);

  for (const group of sortedGroups) {
    const groupData = stats.groups[group];

    markdown += `### ${group.toUpperCase()} (${groupData.count} commands)\n\n`;
    markdown += '| Command | Summary | Since | Complexity |\n';
    markdown += '|---------|---------|-------|------------|\n';

    for (const { name, metadata } of groupData.commands) {
      const summary = (metadata.summary || '').replace(/\|/g, '\\|').substring(0, 80);
      const complexity = (metadata.complexity || 'N/A').replace(/\|/g, '\\|').substring(0, 40);
      const since = metadata.since || 'N/A';
      markdown += `| ${name} | ${summary} | ${since} | ${complexity} |\n`;
    }

    markdown += '\n';
  }

  return markdown;
}

/**
 * Renders a summary as plain text
 */
function renderSummary(stats: CommandStats): void {
  console.log('\n=== Redis Command Compatibility Overview ===\n');
  console.log(`Total Commands: ${stats.totalCommands}\n`);

  console.log('Commands by Group:');
  console.log('─'.repeat(60));

  for (const { group, count, percentage } of stats.groupStats) {
    const bar = '█'.repeat(Math.floor(percentage / 2));
    console.log(`${group.padEnd(20)} ${count.toString().padStart(4)} (${percentage.toFixed(1)}%) ${bar}`);
  }

  console.log('─'.repeat(60));
}

/**
 * Main execution
 */
async function main() {
  const projectRoot = join(import.meta.dir, '..', '..');
  const commandsDir = join(projectRoot, '@commands');

  console.log(`Reading commands from: ${commandsDir}\n`);

  // Read all command files
  const commands = readCommandFiles(commandsDir);
  console.log(`Loaded ${commands.size} commands\n`);

  // Group commands
  const grouped = groupCommandsByType(commands);

  // Generate statistics
  const stats = generateStats(grouped);

  // Display summary
  renderSummary(stats);

  // Generate markdown
  const markdown = renderMarkdown(stats);

  // Write markdown to file
  const outputPath = join(import.meta.dir, 'COMPATIBILITY.md');
  await Bun.write(outputPath, markdown);

  console.log(`\n✓ Markdown report written to: ${outputPath}`);

  // Also write JSON data for programmatic access
  const jsonOutputPath = join(import.meta.dir, 'commands-data.json');
  await Bun.write(jsonOutputPath, JSON.stringify(stats, null, 2));

  console.log(`✓ JSON data written to: ${jsonOutputPath}`);
}

// Run the main function
main().catch(console.error);
