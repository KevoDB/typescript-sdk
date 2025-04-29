/**
 * Example 1: Basic Operations
 * 
 * This example demonstrates the fundamental operations of the Kevo database:
 * - Connecting to the database
 * - Putting values
 * - Getting values
 * - Deleting values
 * - Proper error handling
 * - Disconnecting from the database
 */

const { KevoClient, KeyNotFoundError } = require('../dist');

async function runExample() {
  console.log('EXAMPLE 1: BASIC OPERATIONS');
  console.log('===========================\n');

  // Create a client with connection details and smart query options
  const client = new KevoClient({
    host: 'localhost',
    port: 50051,
    // Smart query options
    autoRouteReads: true,      // Automatically route read operations to replicas if available
    autoRouteWrites: true,     // Automatically route write operations to primary
    preferReplica: true,       // Prefer using a replica for read operations when available
    replicaSelectionStrategy: 'round_robin' // Use round-robin strategy for selecting replicas
  });

  try {
    // Connect to the database
    console.log('Connecting to Kevo database...');
    await client.connect();
    console.log('✓ Connected successfully!\n');

    // Put a string value
    console.log('Storing a string value...');
    await client.put('greeting', 'Hello, Kevo!');
    console.log('✓ Value stored successfully!\n');

    // Get the value back (routing is handled automatically based on client config)
    console.log('Retrieving the value...');
    const value = await client.get('greeting');
    console.log(`✓ Retrieved value: "${value.toString()}" (${value.length} bytes)\n`);

    // Put a JSON value
    console.log('Storing a JSON object...');
    const user = {
      id: 1001,
      name: 'Alice Johnson',
      email: 'alice@example.com',
      created: new Date().toISOString()
    };
    await client.put('user:1001', JSON.stringify(user));
    console.log('✓ JSON object stored successfully!\n');

    // Get and parse the JSON value
    console.log('Retrieving and parsing the JSON object...');
    const userJson = await client.get('user:1001');
    const retrievedUser = JSON.parse(userJson.toString());
    console.log('✓ Retrieved user object:');
    console.log(retrievedUser);
    console.log();

    // Put a binary value
    console.log('Storing binary data...');
    const binaryData = Buffer.from([0x01, 0x02, 0x03, 0x04, 0x05]);
    await client.put('binary-key', binaryData);
    console.log('✓ Binary data stored successfully!\n');

    // Get the binary value
    console.log('Retrieving binary data...');
    const retrievedBinary = await client.get('binary-key');
    console.log(`✓ Retrieved binary data: ${retrievedBinary.toString('hex')} (${retrievedBinary.length} bytes)\n`);

    // Delete a value
    console.log('Deleting a key...');
    await client.delete('greeting');
    console.log('✓ Key deleted successfully!\n');

    // Try to get the deleted value (should throw KeyNotFoundError)
    console.log('Attempting to retrieve deleted key (should fail)...');
    try {
      await client.get('greeting');
      console.log('❌ ERROR: Successfully retrieved a deleted key!');
    } catch (error) {
      if (error instanceof KeyNotFoundError) {
        console.log('✓ Error handled correctly: Key not found error thrown as expected');
      } else {
        console.log(`❌ ERROR: Unexpected error: ${error.message}`);
      }
    }
    console.log();

    // Get database stats (routing is handled automatically)
    try {
      console.log('Retrieving database statistics...');
      const stats = await client.getStats();
      console.log('✓ Database statistics:');
      
      // Database size and structure
      console.log('\n   === Database Structure ===');
      console.log(`   Key count: ${stats.keyCount || 0}`);
      console.log(`   Storage size: ${formatBytes(stats.storageSize || 0)}`);
      console.log(`   Memtables: ${stats.memtableCount || 0}`);
      console.log(`   SSTables: ${stats.sstableCount || 0}`);
      
      // Performance characteristics
      console.log('\n   === Performance Metrics ===');
      console.log(`   Write amplification: ${stats.writeAmplification?.toFixed(2) || '0.00'}`);
      console.log(`   Read amplification: ${stats.readAmplification?.toFixed(2) || '0.00'}`);
      console.log(`   Total bytes read: ${formatBytes(stats.totalBytesRead || 0)}`);
      console.log(`   Total bytes written: ${formatBytes(stats.totalBytesWritten || 0)}`);
      console.log(`   Flush count: ${stats.flushCount || 0}`);
      console.log(`   Compaction count: ${stats.compactionCount || 0}`);
      
      // Operation counts
      if (stats.operationCounts && Object.keys(stats.operationCounts).length > 0) {
        console.log('\n   === Operation Counts ===');
        for (const [op, count] of Object.entries(stats.operationCounts)) {
          console.log(`   ${op}: ${count}`);
        }
      }
      
      // Latency statistics
      if (stats.latencyStats && Object.keys(stats.latencyStats).length > 0) {
        console.log('\n   === Latency Statistics (ns) ===');
        for (const [op, latency] of Object.entries(stats.latencyStats)) {
          console.log(`   ${op}:`);
          console.log(`     Count: ${latency?.count || 0}`);
          console.log(`     Avg: ${formatNanoseconds(latency?.avgNs || 0)}`);
          console.log(`     Min: ${formatNanoseconds(latency?.minNs || 0)}`);
          console.log(`     Max: ${formatNanoseconds(latency?.maxNs || 0)}`);
        }
      }
      
      // Error counts
      if (stats.errorCounts && Object.keys(stats.errorCounts).length > 0) {
        console.log('\n   === Error Counts ===');
        for (const [error, count] of Object.entries(stats.errorCounts)) {
          console.log(`   ${error}: ${count}`);
        }
      }
      
      // Recovery statistics
      if (stats.recoveryStats && stats.recoveryStats.walFilesRecovered > 0) {
        console.log('\n   === Recovery Statistics ===');
        console.log(`   WAL files recovered: ${stats.recoveryStats.walFilesRecovered}`);
        console.log(`   WAL entries recovered: ${stats.recoveryStats.walEntriesRecovered}`);
        console.log(`   Corrupted entries: ${stats.recoveryStats.walCorruptedEntries}`);
        console.log(`   Recovery duration: ${stats.recoveryStats.walRecoveryDurationMs}ms`);
      }
      
      console.log();
    } catch (error) {
      console.log(`Failed to get stats: ${error.message}`);
    }

  } catch (error) {
    console.error('Error:', error.message);
  } finally {
    // Demonstrate scanning (routing is handled automatically)
    console.log('Scanning keys with prefix "user:"...');
    const scanResults = [];
    for await (const item of client.scanPrefix('user:')) {
      scanResults.push({
        key: item.key.toString(),
        value: JSON.parse(item.value.toString())
      });
    }
    console.log(`✓ Found ${scanResults.length} keys with prefix "user:"`);
    console.log(scanResults);
    console.log();

    // Always disconnect when done
    console.log('Disconnecting from database...');
    client.disconnect();
    console.log('✓ Disconnected successfully!');
  }
}

// Helper function to format bytes
function formatBytes(bytes) {
  if (bytes === 0) return '0 Bytes';
  const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB'];
  const i = Math.floor(Math.log(bytes) / Math.log(1024));
  return `${parseFloat((bytes / Math.pow(1024, i)).toFixed(2))} ${sizes[i]}`;
}

// Helper function to format nanoseconds to a more readable format
function formatNanoseconds(ns) {
  if (ns < 1000) {
    return `${ns}ns`;
  } else if (ns < 1000000) {
    return `${(ns / 1000).toFixed(2)}µs`;
  } else if (ns < 1000000000) {
    return `${(ns / 1000000).toFixed(2)}ms`;
  } else {
    return `${(ns / 1000000000).toFixed(2)}s`;
  }
}

// Run the example
runExample().catch(console.error);