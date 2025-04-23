/**
 * Example 2: Transactions
 * 
 * This example demonstrates using transactions with the Kevo database:
 * - Starting transactions (both read-write and read-only)
 * - Reading and writing within a transaction
 * - Committing and rolling back transactions
 * - Transaction isolation
 * - Error handling in transactions
 */

const { KevoClient, KeyNotFoundError } = require('../dist');

async function runExample() {
  console.log('EXAMPLE 2: TRANSACTIONS');
  console.log('=======================\n');

  // Create a client
  const client = new KevoClient({
    host: 'localhost',
    port: 50051,
  });

  try {
    // Connect to the database
    console.log('Connecting to Kevo database...');
    await client.connect();
    console.log('✓ Connected successfully!\n');

    // Setup initial data
    console.log('Setting up initial data...');
    try {
      // First check if counter exists
      try {
        await client.get('counter');
        console.log('Counter already exists, using existing value');
      } catch (error) {
        if (error.message.includes('Key not found')) {
          await client.put('counter', '0');
          console.log('Created new counter with value 0');
        } else {
          throw error;
        }
      }
      
      // Setup user data
      await client.put('user:1', JSON.stringify({ 
        name: 'Alice', 
        email: 'alice@example.com', 
        balance: 100.00 
      }));
      await client.put('user:2', JSON.stringify({ 
        name: 'Bob', 
        email: 'bob@example.com', 
        balance: 50.00 
      }));
      console.log('✓ Initial data created!\n');
    } catch (error) {
      console.error(`Error setting up data: ${error.message}`);
      throw error;
    }

    // Example 1: Basic transaction
    console.log('DEMO 1: Basic Transaction');
    console.log('------------------------');
    console.log('Starting a transaction...');
    const tx1 = await client.beginTransaction();
    console.log(`✓ Transaction started (ID: ${tx1.getId()})`);

    // Read within transaction
    console.log('\nReading counter value...');
    const counterValue = await tx1.get('counter');
    console.log(`✓ Counter value: ${counterValue.toString()}`);

    // Update within transaction
    console.log('\nIncrementing counter...');
    const newValue = (parseInt(counterValue.toString()) + 1).toString();
    await tx1.put('counter', newValue);
    console.log(`✓ Counter updated to ${newValue}`);

    // Verify transaction isolation (uncommitted changes not visible outside transaction)
    console.log('\nVerifying isolation - reading counter outside transaction...');
    const outsideValue = await client.get('counter');
    console.log(`✓ Outside transaction, counter is still: ${outsideValue.toString()}`);

    // Read your own writes within transaction
    console.log('\nReading updated counter value inside transaction...');
    const updatedValue = await tx1.get('counter');
    console.log(`✓ Inside transaction, counter is now: ${updatedValue.toString()}`);

    // Commit transaction
    console.log('\nCommitting transaction...');
    await tx1.commit();
    console.log('✓ Transaction committed!');

    // Verify changes are now visible
    console.log('\nVerifying changes are visible after commit...');
    const afterCommitValue = await client.get('counter');
    console.log(`✓ After commit, counter is: ${afterCommitValue.toString()}\n`);

    // Example 2: Transaction with rollback
    console.log('DEMO 2: Transaction with Rollback');
    console.log('--------------------------------');
    console.log('Starting another transaction...');
    const tx2 = await client.beginTransaction();
    console.log(`✓ Transaction started (ID: ${tx2.getId()})`);

    // Update within transaction
    console.log('\nUpdating counter again...');
    const currentValue = await tx2.get('counter');
    const tempValue = (parseInt(currentValue.toString()) + 10).toString();
    await tx2.put('counter', tempValue);
    console.log(`✓ Counter temporarily updated to ${tempValue}`);

    // Rollback transaction
    console.log('\nRolling back transaction...');
    await tx2.rollback();
    console.log('✓ Transaction rolled back!');

    // Verify changes were discarded
    console.log('\nVerifying changes were discarded...');
    const afterRollbackValue = await client.get('counter');
    console.log(`✓ After rollback, counter is still: ${afterRollbackValue.toString()}\n`);

    // Example 3: Money transfer with transaction
    console.log('DEMO 3: Money Transfer (ACID Example)');
    console.log('-----------------------------------');
    // Get initial balances
    const user1Initial = JSON.parse((await client.get('user:1')).toString());
    const user2Initial = JSON.parse((await client.get('user:2')).toString());
    console.log(`Initial balances - Alice: $${user1Initial.balance.toFixed(2)}, Bob: $${user2Initial.balance.toFixed(2)}`);

    // Transfer money with transaction
    console.log('\nStarting money transfer transaction...');
    const transferTx = await client.beginTransaction();
    
    try {
      console.log('Reading Alice\'s balance...');
      const sender = JSON.parse((await transferTx.get('user:1')).toString());
      
      console.log('Reading Bob\'s balance...');
      const recipient = JSON.parse((await transferTx.get('user:2')).toString());
      
      // Transfer amount
      const amount = 25.00;
      console.log(`\nTransferring $${amount.toFixed(2)} from Alice to Bob...`);
      
      // Check sufficient funds
      if (sender.balance < amount) {
        throw new Error('Insufficient funds');
      }
      
      // Update balances
      sender.balance -= amount;
      recipient.balance += amount;
      
      // Write updated balances
      console.log('Updating Alice\'s balance...');
      await transferTx.put('user:1', JSON.stringify(sender));
      
      console.log('Updating Bob\'s balance...');
      await transferTx.put('user:2', JSON.stringify(recipient));
      
      // Commit the transaction
      console.log('\nCommitting transfer transaction...');
      await transferTx.commit();
      console.log('✓ Transfer completed successfully!');
    } catch (error) {
      // Rollback on error
      console.error(`Error during transfer: ${error.message}`);
      await transferTx.rollback();
      console.log('✗ Transfer failed, transaction rolled back');
    }
    
    // Verify new balances
    console.log('\nVerifying new balances...');
    const user1Final = JSON.parse((await client.get('user:1')).toString());
    const user2Final = JSON.parse((await client.get('user:2')).toString());
    console.log(`Final balances - Alice: $${user1Final.balance.toFixed(2)}, Bob: $${user2Final.balance.toFixed(2)}\n`);

    // Example 4: Read-only transaction
    console.log('DEMO 4: Read-Only Transaction');
    console.log('----------------------------');
    console.log('Starting a read-only transaction...');
    const readOnlyTx = await client.beginTransaction({ readOnly: true });
    console.log(`✓ Read-only transaction started (ID: ${readOnlyTx.getId()})`);

    // Read within read-only transaction
    console.log('\nReading user data within read-only transaction...');
    const aliceData = JSON.parse((await readOnlyTx.get('user:1')).toString());
    console.log(`Read Alice's data: ${JSON.stringify(aliceData)}`);

    // Attempt to write (should fail)
    console.log('\nAttempting to write within read-only transaction (should fail)...');
    try {
      await readOnlyTx.put('test-key', 'test-value');
      console.log('✗ Write succeeded unexpectedly!');
    } catch (error) {
      console.log(`✓ Write failed as expected: ${error.message}`);
    }

    // Commit read-only transaction
    console.log('\nCommitting read-only transaction...');
    await readOnlyTx.commit();
    console.log('✓ Read-only transaction committed!\n');

  } catch (error) {
    console.error('Error:', error.message);
  } finally {
    // Always disconnect when done
    console.log('Disconnecting from database...');
    client.disconnect();
    console.log('✓ Disconnected successfully!');
  }
}

// Run the example
runExample().catch(console.error);