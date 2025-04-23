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

  // Create a client with connection details
  const client = new KevoClient({
    host: 'localhost',
    port: 50051,
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

    // Get the value back
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

    // Get database stats
    try {
      console.log('Retrieving database statistics...');
      const stats = await client.getStats();
      console.log('✓ Database statistics:');
      console.log(`   Total keys: ${stats.totalKeys}`);
      console.log(`   Disk usage: ${formatBytes(stats.diskUsageBytes)}`);
      
      // These fields might not be available depending on the server implementation
      if (stats.memoryUsageBytes) {
        console.log(`   Memory usage: ${formatBytes(stats.memoryUsageBytes)}`);
      }
      if (stats.version) {
        console.log(`   Version: ${stats.version}`);
      }
      if (stats.uptime) {
        console.log(`   Uptime: ${stats.uptime}`);
      }
      if (stats.lastCompactionTime) {
        console.log(`   Last compaction: ${stats.lastCompactionTime}`);
      }
      console.log();
    } catch (error) {
      console.log(`Failed to get stats: ${error.message}`);
    }

  } catch (error) {
    console.error('Error:', error.message);
  } finally {
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

// Run the example
runExample().catch(console.error);