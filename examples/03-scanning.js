/**
 * Example 3: Scanning Operations
 * 
 * This example demonstrates different scanning methods with the Kevo database:
 * - Prefix scanning (finding all keys with a specific prefix)
 * - Range scanning (finding keys within a range)
 * - Using scan options (limit, reverse order)
 * - Working with scan results
 */

const { KevoClient } = require('../dist');

async function runExample() {
  console.log('EXAMPLE 3: SCANNING OPERATIONS');
  console.log('=============================\n');

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

    // Setup sample data
    console.log('Setting up sample data...');
    
    // Clear any existing data with these prefixes
    await clearExistingData(client);

    // Setup user data
    const users = [
      { id: 'user:1001', name: 'Alice Johnson', email: 'alice@example.com', age: 28 },
      { id: 'user:1002', name: 'Bob Smith', email: 'bob@example.com', age: 35 },
      { id: 'user:1003', name: 'Carol Davis', email: 'carol@example.com', age: 42 },
      { id: 'user:1004', name: 'Dave Wilson', email: 'dave@example.com', age: 23 },
      { id: 'user:1005', name: 'Eve Brown', email: 'eve@example.com', age: 31 }
    ];
    
    // Setup product data
    const products = [
      { id: 'product:101', name: 'Smartphone', category: 'electronics', price: 599.99 },
      { id: 'product:102', name: 'Laptop', category: 'electronics', price: 1299.99 },
      { id: 'product:103', name: 'Headphones', category: 'electronics', price: 149.99 },
      { id: 'product:201', name: 'T-shirt', category: 'clothing', price: 24.99 },
      { id: 'product:202', name: 'Jeans', category: 'clothing', price: 59.99 }
    ];
    
    // Setup order data
    const orders = [
      { id: 'order:5001', userId: 'user:1001', items: 3, total: 74.97, status: 'delivered' },
      { id: 'order:5002', userId: 'user:1002', items: 1, total: 1299.99, status: 'shipped' },
      { id: 'order:5003', userId: 'user:1001', items: 2, total: 749.98, status: 'processing' },
      { id: 'order:5004', userId: 'user:1004', items: 4, total: 124.96, status: 'delivered' },
      { id: 'order:5005', userId: 'user:1003', items: 1, total: 59.99, status: 'processing' }
    ];
    
    // Store all data
    const batch = client.batch();
    
    for (const user of users) {
      batch.put(user.id, JSON.stringify(user));
    }
    
    for (const product of products) {
      batch.put(product.id, JSON.stringify(product));
    }
    
    for (const order of orders) {
      batch.put(order.id, JSON.stringify(order));
    }
    
    // Add some sorted data for range demonstration
    for (let i = 1; i <= 20; i++) {
      batch.put(`sorted:${i.toString().padStart(2, '0')}`, `Value ${i}`);
    }
    
    await batch.execute();
    console.log(`✓ Stored ${users.length + products.length + orders.length + 20} key-value pairs!\n`);

    // DEMO 1: Prefix Scanning - Basic
    console.log('DEMO 1: Basic Prefix Scanning');
    console.log('----------------------------');
    console.log('Scanning for all user data...');
    
    let count = 0;
    for await (const { key, value } of client.scanPrefix('user:')) {
      const userData = JSON.parse(value.toString());
      console.log(`- ${key.toString()}: ${userData.name}, ${userData.email}`);
      count++;
    }
    
    console.log(`✓ Found ${count} user records\n`);

    // DEMO 2: Prefix Scanning with Limit
    console.log('DEMO 2: Prefix Scanning with Limit');
    console.log('--------------------------------');
    console.log('Scanning for first 3 products...');
    
    count = 0;
    for await (const { key, value } of client.scanPrefix('product:', { limit: 3 })) {
      const productData = JSON.parse(value.toString());
      console.log(`- ${key.toString()}: ${productData.name}, $${productData.price}`);
      count++;
    }
    
    console.log(`✓ Retrieved ${count} products (limited to 3)\n`);

    // DEMO 3: Prefix Scanning in Reverse Order
    console.log('DEMO 3: Reverse Prefix Scanning');
    console.log('------------------------------');
    console.log('Scanning for all orders in reverse order...');
    
    count = 0;
    for await (const { key, value } of client.scanPrefix('order:', { reverse: true })) {
      const orderData = JSON.parse(value.toString());
      console.log(`- ${key.toString()}: User ${orderData.userId}, Total: $${orderData.total}, Status: ${orderData.status}`);
      count++;
    }
    
    console.log(`✓ Retrieved ${count} orders in reverse order\n`);

    // DEMO 4: Range Scanning - Basic
    console.log('DEMO 4: Basic Range Scanning');
    console.log('---------------------------');
    console.log('Scanning for values between sorted:05 and sorted:15...');
    
    count = 0;
    try {
      for await (const { key, value } of client.scanRange('sorted:05', 'sorted:15')) {
        if (key.toString().startsWith('sorted:')) {
          console.log(`- ${key.toString()}: ${value.toString()}`);
          count++;
        }
      }
    } catch (error) {
      console.error(`Error during range scan: ${error.message}`);
    }
    
    console.log(`✓ Retrieved ${count} values in the specified range\n`);

    // DEMO 5: Range Scanning with Limit
    console.log('DEMO 5: Range Scanning with Limit');
    console.log('-------------------------------');
    console.log('Scanning for first 3 values between sorted:05 and sorted:15...');
    
    count = 0;
    for await (const { key, value } of client.scanRange('sorted:05', 'sorted:15', { limit: 3 })) {
      if (key.toString().startsWith('sorted:')) {
        console.log(`- ${key.toString()}: ${value.toString()}`);
        count++;
        if (count >= 3) break; // Ensure we only get 3 items that match our prefix
      }
    }
    
    console.log(`✓ Retrieved ${count} values (limited to 3)\n`);

    // DEMO 6: Range Scanning in Reverse Order
    console.log('DEMO 6: Reverse Range Scanning');
    console.log('-----------------------------');
    console.log('Scanning for values between sorted:05 and sorted:15 in reverse order...');
    
    count = 0;
    try {
      for await (const { key, value } of client.scanRange('sorted:05', 'sorted:15', { reverse: true })) {
        if (key.toString().startsWith('sorted:')) {
          console.log(`- ${key.toString()}: ${value.toString()}`);
          count++;
        }
      }
    } catch (error) {
      console.error(`Error during reverse range scan: ${error.message}`);
    }
    
    console.log(`✓ Retrieved ${count} values in reverse order\n`);

    // DEMO 7: Category Filtering with Prefix Scan
    console.log('DEMO 7: Filtering with Prefix (Category Example)');
    console.log('---------------------------------------------');
    console.log('Getting all products in the "electronics" category:');
    
    // First scan for all products
    const electronics = [];
    const seenKeys = new Set(); // To handle duplicates in our test DB
    
    for await (const { key, value } of client.scanPrefix('product:')) {
      // Skip duplicates
      const keyStr = key.toString();
      if (seenKeys.has(keyStr)) continue;
      seenKeys.add(keyStr);
      
      try {
        const product = JSON.parse(value.toString());
        // Filter by category
        if (product.category === 'electronics') {
          electronics.push(product);
        }
      } catch (error) {
        console.error(`Error parsing product JSON: ${error.message}`);
      }
    }
    
    console.log(`✓ Found ${electronics.length} electronics products:`);
    electronics.forEach(product => {
      console.log(`- ${product.name}: $${product.price}`);
    });
    console.log();

    // DEMO 8: Finding User Orders with Prefix Scan
    console.log('DEMO 8: Finding User Orders (Relationship Example)');
    console.log('-----------------------------------------------');
    const targetUserId = 'user:1001';
    console.log(`Getting all orders for user ID: ${targetUserId}`);
    
    try {
      // Get user details
      const userData = JSON.parse((await client.get(targetUserId)).toString());
      console.log(`User: ${userData.name}`);
      
      // Scan all orders and filter
      const userOrders = [];
      const seenOrderKeys = new Set(); // To handle duplicates in our test DB
      
      for await (const { key, value } of client.scanPrefix('order:')) {
        // Skip duplicates
        const keyStr = key.toString();
        if (seenOrderKeys.has(keyStr)) continue;
        seenOrderKeys.add(keyStr);
        
        try {
          const order = JSON.parse(value.toString());
          // Filter by user ID
          if (order.userId === targetUserId) {
            userOrders.push(order);
          }
        } catch (error) {
          console.error(`Error parsing order JSON: ${error.message}`);
        }
      }
      
      console.log(`✓ Found ${userOrders.length} orders for this user:`);
      userOrders.forEach(order => {
        console.log(`- Order ${order.id}: ${order.items} items, Total: $${order.total}, Status: ${order.status}`);
      });
    } catch (error) {
      console.error(`Error fetching user orders: ${error.message}`);
    }
    console.log();

    // DEMO 9: Transaction with Scan (Consistent Read)
    console.log('DEMO 9: Transaction with Scanning');
    console.log('-------------------------------');
    console.log('Starting a read-only transaction...');
    
    const tx = await client.beginTransaction({ readOnly: true });
    console.log(`✓ Transaction started (ID: ${tx.getId()})`);
    
    // Use scan within a transaction
    console.log('\nScanning for clothing products within transaction...');
    
    const clothingProducts = [];
    const seenTxKeys = new Set(); // To handle duplicates in our test DB
    
    try {
      for await (const { key, value } of tx.scan({ prefix: 'product:' })) {
        // Skip duplicates
        const keyStr = key.toString();
        if (seenTxKeys.has(keyStr)) continue;
        seenTxKeys.add(keyStr);
        
        try {
          const product = JSON.parse(value.toString());
          // Filter by category
          if (product.category === 'clothing') {
            clothingProducts.push(product);
          }
        } catch (error) {
          console.error(`Error parsing product JSON in transaction: ${error.message}`);
        }
      }
      
      console.log(`✓ Found ${clothingProducts.length} clothing products within transaction:`);
      clothingProducts.forEach(product => {
        console.log(`- ${product.name}: $${product.price}`);
      });
      
      // Commit transaction
      console.log('\nCommitting transaction...');
      await tx.commit();
      console.log('✓ Transaction committed!');
    } catch (error) {
      console.error(`Transaction error: ${error.message}`);
      await tx.rollback();
      console.log('Transaction rolled back due to error');
    }

  } catch (error) {
    console.error('Error:', error.message);
  } finally {
    // Always disconnect when done
    console.log('\nDisconnecting from database...');
    client.disconnect();
    console.log('✓ Disconnected successfully!');
  }
}

// Helper to clear existing data
async function clearExistingData(client) {
  const prefixesToClear = ['user:', 'product:', 'order:', 'sorted:'];
  const batch = client.batch();
  let count = 0;
  
  for (const prefix of prefixesToClear) {
    for await (const { key } of client.scanPrefix(prefix)) {
      batch.delete(key.toString());
      count++;
    }
  }
  
  if (count > 0) {
    await batch.execute();
    console.log(`Cleared ${count} existing keys`);
  }
}

// Run the example
runExample().catch(console.error);