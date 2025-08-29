#!/usr/bin/env node

const fs = require('fs');
const path = require('path');

// Files to include in the combined output
const filesToInclude = [
  'src/lib/ChunkingEngine.ts',
  'src/lib/DataInsertionEngine.ts', 
  'src/lib/PaginationAnalyzer.ts',
  'src/lib/SchemaInferenceEngine.ts',
  'src/lib/SchemaParser.ts',
  'src/lib/types.ts',
  'src/index.ts',
  'src/do.ts',
  'wrangler.jsonc'
];

// Output file
const outputFile = 'combined-source.txt';

function generateFileTree() {
  let tree = 'PROJECT FILE TREE\n';
  tree += '=================\n\n';
  tree += 'open-targets-mcp-server/\n';
  tree += '├── src/\n';
  tree += '│   ├── lib/\n';
  tree += '│   │   ├── ChunkingEngine.ts\n';
  tree += '│   │   ├── DataInsertionEngine.ts\n';
  tree += '│   │   ├── PaginationAnalyzer.ts\n';
  tree += '│   │   ├── SchemaInferenceEngine.ts\n';
  tree += '│   │   ├── SchemaParser.ts\n';
  tree += '│   │   └── types.ts\n';
  tree += '│   ├── index.ts\n';
  tree += '│   └── do.ts\n';
  tree += '└── wrangler.jsonc\n\n';
  return tree;
}

function generateFileHeader(filePath) {
  const separator = '='.repeat(80);
  return `${separator}\n` +
         `FILE: ${filePath}\n` +
         `${separator}\n\n`;
}

function generateFileFooter(filePath) {
  const separator = '-'.repeat(80);
  return `\n${separator}\n` +
         `END OF FILE: ${filePath}\n` +
         `${separator}\n\n\n`;
}

function combineFiles() {
  console.log('Combining source files...');
  
  let combinedContent = '';
  
  // Add file tree at the top
  combinedContent += generateFileTree();
  
  // Process each file
  for (const filePath of filesToInclude) {
    try {
      if (fs.existsSync(filePath)) {
        console.log(`Processing: ${filePath}`);
        
        // Add file header
        combinedContent += generateFileHeader(filePath);
        
        // Read and add file content
        const content = fs.readFileSync(filePath, 'utf8');
        combinedContent += content;
        
        // Add file footer
        combinedContent += generateFileFooter(filePath);
      } else {
        console.warn(`Warning: File not found: ${filePath}`);
        combinedContent += generateFileHeader(filePath);
        combinedContent += `[FILE NOT FOUND]\n`;
        combinedContent += generateFileFooter(filePath);
      }
    } catch (error) {
      console.error(`Error processing ${filePath}:`, error.message);
      combinedContent += generateFileHeader(filePath);
      combinedContent += `[ERROR READING FILE: ${error.message}]\n`;
      combinedContent += generateFileFooter(filePath);
    }
  }
  
  // Write combined content to output file
  try {
    fs.writeFileSync(outputFile, combinedContent, 'utf8');
    console.log(`\nSuccess! Combined file created: ${outputFile}`);
    console.log(`Total files processed: ${filesToInclude.length}`);
    
    // Show file size info
    const stats = fs.statSync(outputFile);
    console.log(`Output file size: ${(stats.size / 1024).toFixed(2)} KB`);
  } catch (error) {
    console.error('Error writing output file:', error.message);
    process.exit(1);
  }
}

// Add timestamp to the beginning
function addTimestamp() {
  const timestamp = new Date().toISOString();
  let header = `COMBINED SOURCE FILES\n`;
  header += `Generated: ${timestamp}\n`;
  header += `${'='.repeat(50)}\n\n`;
  return header;
}

// Main execution
function main() {
  console.log('Starting file combination process...');
  
  // Add timestamp header
  let finalContent = addTimestamp();
  
  // Add file tree
  finalContent += generateFileTree();
  
  // Process each file
  for (const filePath of filesToInclude) {
    try {
      if (fs.existsSync(filePath)) {
        console.log(`Processing: ${filePath}`);
        
        finalContent += generateFileHeader(filePath);
        const content = fs.readFileSync(filePath, 'utf8');
        finalContent += content;
        finalContent += generateFileFooter(filePath);
      } else {
        console.warn(`Warning: File not found: ${filePath}`);
        finalContent += generateFileHeader(filePath);
        finalContent += `[FILE NOT FOUND]\n`;
        finalContent += generateFileFooter(filePath);
      }
    } catch (error) {
      console.error(`Error processing ${filePath}:`, error.message);
      finalContent += generateFileHeader(filePath);
      finalContent += `[ERROR READING FILE: ${error.message}]\n`;
      finalContent += generateFileFooter(filePath);
    }
  }
  
  // Write final output
  try {
    fs.writeFileSync(outputFile, finalContent, 'utf8');
    console.log(`\nSuccess! Combined file created: ${outputFile}`);
    console.log(`Total files processed: ${filesToInclude.length}`);
    
    const stats = fs.statSync(outputFile);
    console.log(`Output file size: ${(stats.size / 1024).toFixed(2)} KB`);
  } catch (error) {
    console.error('Error writing output file:', error.message);
    process.exit(1);
  }
}

// Run the script
main(); 