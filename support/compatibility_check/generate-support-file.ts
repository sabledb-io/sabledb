#!/usr/bin/env bun

import { readdirSync, readFileSync } from 'fs';
import { join } from 'path';

interface CommandFile {
  [commandName: string]: any;
}

interface SupportEntry {
  name: string;
  supported: boolean;
  notes: string;
}

/**
 * Reads all JSON files from the @commands directory and extracts command names
 */
function readCommandNames(commandsDir: string): string[] {
  const commandNames: string[] = [];

  const files = readdirSync(commandsDir).filter(f => f.endsWith('.json'));

  console.log(`Found ${files.length} JSON files in ${commandsDir}`);

  for (const file of files) {
    try {
      const filePath = join(commandsDir, file);
      const content = readFileSync(filePath, 'utf-8');
      const commandFile: CommandFile = JSON.parse(content);

      // Each file contains one command with the command name as the key
      for (const commandName of Object.keys(commandFile)) {
        commandNames.push(commandName);
      }
    } catch (error) {
      console.error(`Error reading file ${file}:`, error);
    }
  }

  return commandNames;
}

/**
 * Generates the initial support file with all commands marked as unsupported
 */
function generateSupportEntries(commandNames: string[]): SupportEntry[] {
  const entries: SupportEntry[] = commandNames.map(name => ({
    name,
    supported: false,
    notes: ''
  }));

  // Sort alphabetically by command name
  entries.sort((a, b) => a.name.localeCompare(b.name));

  return entries;
}

/**
 * Converts support entries to JSONL format
 */
function toJSONL(entries: SupportEntry[]): string {
  return entries.map(entry => JSON.stringify(entry)).join('\n');
}

/**
 * Main execution
 */
async function main() {
  const projectRoot = join(import.meta.dir, '..', '..');
  const commandsDir = join(projectRoot, '@commands');
  const outputPath = join(import.meta.dir, 'sabledb-support.jsonl');

  console.log(`Reading commands from: ${commandsDir}\n`);

  // Read all command names
  const commandNames = readCommandNames(commandsDir);
  console.log(`Found ${commandNames.length} commands\n`);

  // Generate support entries (all unsupported by default)
  const entries = generateSupportEntries(commandNames);

  // Convert to JSONL format
  const jsonl = toJSONL(entries);

  // Write to file
  await Bun.write(outputPath, jsonl + '\n'); // Add trailing newline

  console.log(`✓ Generated ${outputPath}`);
  console.log(`  Total commands: ${entries.length}`);
  console.log(`  All marked as supported: false`);
  console.log(`\nNext steps:`);
  console.log(`  1. Edit sabledb-support.jsonl to mark supported commands`);
  console.log(`  2. Add notes for unsupported commands`);
  console.log(`  3. Run the main script to generate compatibility report`);
}

// Run the main function
main().catch(console.error);
