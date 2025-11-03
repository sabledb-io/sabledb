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
}

// SableDB support information
interface SupportInfo {
  name: string;
  supported: boolean;
  notes: string;
}

// Combined command with support data
interface CommandWithSupport {
  name: string;
  metadata: CommandMetadata;
  supported: boolean;
  supportNotes: string;
}

interface CommandFile {
  [commandName: string]: CommandMetadata;
}

interface GroupedCommands {
  [group: string]: {
    commands: Array<CommandWithSupport>;
    count: number;
    supportedCount: number;
  };
}

interface CommandStats {
  totalCommands: number;
  supportedCommands: number;
  unsupportedCommands: number;
  supportPercentage: number;
  groups: GroupedCommands;
  groupStats: Array<{
    group: string;
    count: number;
    supportedCount: number;
    percentage: number;
    supportPercentage: number;
  }>;
}

/**
 * Reads the SableDB support file (JSONL format)
 */
function readSupportFile(supportFilePath: string): Map<string, SupportInfo> {
  const supportMap = new Map<string, SupportInfo>();

  try {
    const content = readFileSync(supportFilePath, 'utf-8');
    const lines = content.trim().split('\n');

    for (const line of lines) {
      if (line.trim()) {
        const support: SupportInfo = JSON.parse(line);
        supportMap.set(support.name, support);
      }
    }

    console.log(`Loaded ${supportMap.size} support entries from ${supportFilePath}`);
  } catch (error) {
    console.warn(`Warning: Could not read support file ${supportFilePath}:`, error);
    console.warn('Continuing without support data...');
  }

  return supportMap;
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
 * Merges command metadata with support information
 */
function mergeCommandsWithSupport(
  commands: Map<string, CommandMetadata>,
  supportMap: Map<string, SupportInfo>
): Map<string, CommandWithSupport> {
  const merged = new Map<string, CommandWithSupport>();

  for (const [commandName, metadata] of commands.entries()) {
    const support = supportMap.get(commandName);

    merged.set(commandName, {
      name: commandName,
      metadata,
      supported: support?.supported ?? false,
      supportNotes: support?.notes ?? ''
    });
  }

  return merged;
}

/**
 * Groups commands by their group/category
 */
function groupCommandsByType(commands: Map<string, CommandWithSupport>): GroupedCommands {
  const grouped: GroupedCommands = {};

  for (const [commandName, command] of commands.entries()) {
    const group = command.metadata.group || 'unknown';

    if (!grouped[group]) {
      grouped[group] = {
        commands: [],
        count: 0,
        supportedCount: 0
      };
    }

    grouped[group].commands.push(command);
    grouped[group].count++;
    if (command.supported) {
      grouped[group].supportedCount++;
    }
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
  const supportedCommands = Object.values(grouped).reduce((sum, g) => sum + g.supportedCount, 0);
  const unsupportedCommands = totalCommands - supportedCommands;

  const groupStats = Object.entries(grouped)
    .map(([group, data]) => ({
      group,
      count: data.count,
      supportedCount: data.supportedCount,
      percentage: (data.count / totalCommands) * 100,
      supportPercentage: data.count > 0 ? (data.supportedCount / data.count) * 100 : 0
    }))
    .sort((a, b) => b.count - a.count);

  return {
    totalCommands,
    supportedCommands,
    unsupportedCommands,
    supportPercentage: totalCommands > 0 ? (supportedCommands / totalCommands) * 100 : 0,
    groups: grouped,
    groupStats
  };
}

/**
 * Renders the data as a markdown table
 */
function renderMarkdown(stats: CommandStats): string {
  let markdown = '# SableDB Redis Compatibility Report\n\n';

  // Overall statistics
  markdown += '## Overall Compatibility\n\n';
  markdown += `- **Total Commands:** ${stats.totalCommands}\n`;
  markdown += `- **Supported:** ${stats.supportedCommands} (${stats.supportPercentage.toFixed(2)}%)\n`;
  markdown += `- **Not Supported:** ${stats.unsupportedCommands} (${(100 - stats.supportPercentage).toFixed(2)}%)\n\n`;

  // Group statistics table
  markdown += '## Commands by Group\n\n';
  markdown += '| Group | Total | Supported | Support % |\n';
  markdown += '|-------|-------|-----------|----------|\n';

  for (const { group, count, supportedCount, supportPercentage } of stats.groupStats) {
    const supportIcon = supportPercentage === 100 ? '✅' : supportPercentage === 0 ? '❌' : '🟡';
    markdown += `| ${group} | ${count} | ${supportedCount} | ${supportPercentage.toFixed(1)}% ${supportIcon} |\n`;
  }

  markdown += '\n';

  // Detailed breakdown by group
  markdown += '## Detailed Command List\n\n';

  const sortedGroups = stats.groupStats.map(s => s.group);

  for (const group of sortedGroups) {
    const groupData = stats.groups[group];

    markdown += `### ${group.toUpperCase()} (${groupData.supportedCount}/${groupData.count} supported)\n\n`;
    markdown += '| Command | Supported | Summary | Notes |\n';
    markdown += '|---------|-----------|---------|-------|\n';

    for (const command of groupData.commands) {
      const status = command.supported ? '✅' : '❌';
      const summary = (command.metadata.summary || '').replace(/\|/g, '\\|').substring(0, 60);
      const notes = command.supportNotes.replace(/\|/g, '\\|').substring(0, 50);
      markdown += `| ${command.name} | ${status} | ${summary} | ${notes} |\n`;
    }

    markdown += '\n';
  }

  return markdown;
}

/**
 * Renders a summary as plain text
 */
function renderSummary(stats: CommandStats): void {
  console.log('\n=== SableDB Redis Compatibility Report ===\n');

  console.log('Overall Compatibility:');
  console.log(`  Total Commands:     ${stats.totalCommands}`);
  console.log(`  ✅ Supported:       ${stats.supportedCommands} (${stats.supportPercentage.toFixed(1)}%)`);
  console.log(`  ❌ Not Supported:   ${stats.unsupportedCommands} (${(100 - stats.supportPercentage).toFixed(1)}%)`);
  console.log('');

  console.log('Commands by Group:');
  console.log('─'.repeat(70));
  console.log(`${'Group'.padEnd(20)} ${'Total'.padStart(5)} ${'Supp.'.padStart(5)} ${'%'.padStart(6)} Progress`);
  console.log('─'.repeat(70));

  for (const { group, count, supportedCount, supportPercentage } of stats.groupStats) {
    const bar = '█'.repeat(Math.floor(supportPercentage / 5));
    const emptyBar = '░'.repeat(20 - Math.floor(supportPercentage / 5));
    const icon = supportPercentage === 100 ? '✅' : supportPercentage === 0 ? '❌' : '🟡';
    console.log(
      `${group.padEnd(20)} ${count.toString().padStart(5)} ${supportedCount.toString().padStart(5)} ` +
      `${supportPercentage.toFixed(1).padStart(5)}% ${icon} ${bar}${emptyBar}`
    );
  }

  console.log('─'.repeat(70));
}

/**
 * Main execution
 */
async function main() {
  const projectRoot = join(import.meta.dir, '..', '..');
  const commandsDir = join(projectRoot, '@commands');
  const supportFilePath = join(import.meta.dir, 'sabledb-support.jsonl');

  console.log(`Reading commands from: ${commandsDir}`);
  console.log(`Reading support data from: ${supportFilePath}\n`);

  // Read all command files
  const commands = readCommandFiles(commandsDir);
  console.log(`Loaded ${commands.size} commands`);

  // Read support file
  const supportMap = readSupportFile(supportFilePath);
  console.log('');

  // Merge commands with support data
  const commandsWithSupport = mergeCommandsWithSupport(commands, supportMap);

  // Group commands
  const grouped = groupCommandsByType(commandsWithSupport);

  // Generate statistics
  const stats = generateStats(grouped);

  // Display summary
  renderSummary(stats);

  // Generate markdown
  const markdown = renderMarkdown(stats);

  // Write markdown to file in docs folder
  const docsPath = join(projectRoot, 'docs', 'COMPATIBILITY.md');
  await Bun.write(docsPath, markdown);

  console.log(`\n✓ Markdown report written to: ${docsPath}`);

  // Also write JSON data for programmatic access
  const jsonOutputPath = join(import.meta.dir, 'commands-data.json');
  await Bun.write(jsonOutputPath, JSON.stringify(stats, null, 2));

  console.log(`✓ JSON data written to: ${jsonOutputPath}`);
}

// Run the main function
main().catch(console.error);
