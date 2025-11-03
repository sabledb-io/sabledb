#!/usr/bin/env bun

import { readFileSync } from 'fs';
import { join } from 'path';

interface ReadmeCommand {
  supported: boolean;
  fully_supported: boolean;
  notes: string;
}

interface ReadmeCommands {
  [key: string]: ReadmeCommand;
}

interface SupportEntry {
  name: string;
  supported: boolean;
  notes: string;
}

/**
 * Reads the readme-commands.json file
 */
function readReadmeCommands(filePath: string): ReadmeCommands {
  const content = readFileSync(filePath, 'utf-8');
  return JSON.parse(content);
}

/**
 * Reads the existing sabledb-support.jsonl file
 */
function readExistingSupport(filePath: string): Map<string, SupportEntry> {
  const supportMap = new Map<string, SupportEntry>();

  try {
    const content = readFileSync(filePath, 'utf-8');
    const lines = content.trim().split('\n');

    for (const line of lines) {
      if (line.trim()) {
        const entry: SupportEntry = JSON.parse(line);
        supportMap.set(entry.name, entry);
      }
    }
  } catch (error) {
    console.log('No existing support file found or error reading it');
  }

  return supportMap;
}

/**
 * Merges README commands with existing support data
 */
function mergeCommands(
  readmeCommands: ReadmeCommands,
  existingSupport: Map<string, SupportEntry>
): Map<string, SupportEntry> {

  const merged = new Map<string, SupportEntry>();

  // First, add all existing entries (this preserves any custom entries)
  for (const [name, entry] of existingSupport.entries()) {
    merged.set(name, entry);
  }

  // Then, update/add from README
  for (const [commandName, commandInfo] of Object.entries(readmeCommands)) {
    const name = commandName.toUpperCase();

    // Build notes string
    let notes = commandInfo.notes;
    if (!commandInfo.fully_supported && !notes) {
      notes = 'Partially supported';
    }

    merged.set(name, {
      name,
      supported: commandInfo.supported,
      notes
    });
  }

  return merged;
}

/**
 * Converts the merged data to JSONL format (sorted alphabetically)
 */
function toJSONL(entries: Map<string, SupportEntry>): string {
  const sorted = Array.from(entries.values()).sort((a, b) =>
    a.name.localeCompare(b.name)
  );

  return sorted.map(entry => JSON.stringify(entry)).join('\n');
}

/**
 * Main execution
 */
async function main() {
  const readmeCommandsPath = join(import.meta.dir, 'readme-commands.json');
  const supportFilePath = join(import.meta.dir, 'sabledb-support.jsonl');

  console.log('Reading README commands...');
  const readmeCommands = readReadmeCommands(readmeCommandsPath);
  console.log(`Found ${Object.keys(readmeCommands).length} commands in README\n`);

  console.log('Reading existing support data...');
  const existingSupport = readExistingSupport(supportFilePath);
  console.log(`Found ${existingSupport.size} existing entries\n`);

  console.log('Merging data...');
  const merged = mergeCommands(readmeCommands, existingSupport);
  console.log(`Total entries after merge: ${merged.size}\n`);

  // Generate statistics
  let supportedCount = 0;
  let unsupportedCount = 0;
  let withNotesCount = 0;

  for (const entry of merged.values()) {
    if (entry.supported) {
      supportedCount++;
    } else {
      unsupportedCount++;
    }
    if (entry.notes) {
      withNotesCount++;
    }
  }

  console.log('Statistics:');
  console.log(`  ✅ Supported: ${supportedCount}`);
  console.log(`  ❌ Not supported: ${unsupportedCount}`);
  console.log(`  📝 With notes: ${withNotesCount}\n`);

  // Convert to JSONL
  const jsonl = toJSONL(merged);

  // Write to file
  await Bun.write(supportFilePath, jsonl + '\n');

  console.log(`✓ Updated ${supportFilePath}`);
  console.log(`\nNext step: Run 'bun run check' to generate the compatibility report`);
}

// Run the main function
main().catch(console.error);
