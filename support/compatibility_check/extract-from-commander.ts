#!/usr/bin/env bun

import { readFileSync } from 'fs';
import { join } from 'path';

interface SupportEntry {
  name: string;
  supported: boolean;
  notes: string;
}

/**
 * Extract all registered commands from commander.rs
 */
function extractCommandsFromCommander(): Set<string> {
  const commanderPath = join(
    import.meta.dir,
    '..',
    '..',
    'crates',
    'libsabledb',
    'src',
    'commands',
    'commander.rs'
  );

  const content = readFileSync(commanderPath, 'utf-8');
  const commands = new Set<string>();

  // Parse the HashMap::from([ ... ]) section
  const lines = content.split('\n');
  let inHashMap = false;
  let nextLineIsCommand = false;

  for (let i = 0; i < lines.length; i++) {
    const line = lines[i].trim();

    // Start of the HashMap
    if (line.includes('HashMap::from([')) {
      inHashMap = true;
      continue;
    }

    // End of HashMap
    if (inHashMap && line === ']);') {
      break;
    }

    // Look for opening paren indicating start of new entry
    if (inHashMap && line === '(') {
      nextLineIsCommand = true;
      continue;
    }

    // Extract command name from next line after '('
    if (inHashMap && nextLineIsCommand) {
      const match = line.match(/"([^"]+)"/);
      if (match) {
        commands.add(match[1].toUpperCase());
      }
      nextLineIsCommand = false;
    }
  }

  return commands;
}

/**
 * Read existing support file
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
    console.log('No existing support file found');
  }

  return supportMap;
}

/**
 * Main execution
 */
async function main() {
  console.log('Extracting commands from commander.rs...\n');

  // Extract from commander.rs
  const commanderCommands = extractCommandsFromCommander();
  console.log(`Found ${commanderCommands.size} commands in commander.rs`);

  // Read existing support data
  const supportFilePath = join(import.meta.dir, 'sabledb-support.jsonl');
  const existingSupport = readExistingSupport(supportFilePath);
  console.log(`Found ${existingSupport.size} existing entries\n`);

  // Update support status for commands found in commander.rs
  let updated = 0;
  let alreadySupported = 0;

  for (const cmdName of commanderCommands) {
    const existing = existingSupport.get(cmdName);

    if (existing) {
      if (!existing.supported) {
        existing.supported = true;
        updated++;
      } else {
        alreadySupported++;
      }
    } else {
      // New command not in our list
      existingSupport.set(cmdName, {
        name: cmdName,
        supported: true,
        notes: ''
      });
      updated++;
    }
  }

  console.log('Update Summary:');
  console.log(`  ✅ Commands marked as supported: ${updated}`);
  console.log(`  ℹ️  Already marked as supported: ${alreadySupported}`);
  console.log(`  📝 Total supported commands: ${commanderCommands.size}\n`);

  // Sort alphabetically
  const sortedEntries = Array.from(existingSupport.values()).sort((a, b) =>
    a.name.localeCompare(b.name)
  );

  // Convert to JSONL
  const jsonl = sortedEntries.map(entry => JSON.stringify(entry)).join('\n');

  // Write to file
  await Bun.write(supportFilePath, jsonl + '\n');

  console.log(`✓ Updated ${supportFilePath}`);
  console.log(`\nTotal entries: ${sortedEntries.length}`);
  console.log(`Supported: ${sortedEntries.filter(e => e.supported).length}`);
  console.log(`Not supported: ${sortedEntries.filter(e => !e.supported).length}`);
}

// Run the main function
main().catch(console.error);
