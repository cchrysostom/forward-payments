"use strict";

let sqlite3 = require("sqlite3").verbose();
let blocktrail = require("blocktrail-sdk");

let payprocDb = new sqlite3.Database("/home/chris/Documents/Alexandria/payproc/payproc.db");

let query = `select PaymentAddress.destinationAddress, PaymentAddress.paymentAddress, PaymentAddress.targetBalance, PaymentAddress.payableBalance,  
       PaymentAddress.status, PaymentAddress.forwarded, AddressBalance.balance, AddressBalance.balanceDate
  from PaymentAddress join AddressBalance using (paymentAddress)
  where AddressBalance.balance > 0
    and PaymentAddress.forwarded = 0
  order by PaymentAddress.destinationAddress, AddressBalance.balance desc`;

let fwdPymtAllowed = true;

function sendTransaction(wallet, address, amount, btcPrice) {
    if (!fwdPymtAllowed) {
      setTimeout(sendTransaction, 5000, wallet, address, amount, btcPrice);
      return;
    }

    if (amount < pricePerDollar(btcPrice)) {
      console.log("Skipping paying " + amount + " to " + address + ". Threshold is " + pricePerDollar(btcPrice) + ".");
      return;
    }

    fwdPymtAllowed = false;
    let pay = {};
    pay[address] = amount;
    wallet.unlock({passphrase: process.env.WALLET_PASSWORD}, function(err) {
      if (err) {
        console.log("Wallet unlock error: ", err, address, amount);
        return;
      }

      wallet.pay(pay)
        .then(function(txid) {
          console.log("Payment made with transaction, " + txid + ". Time: " + Date.now());
          updateForwardedStatus(address);
          fwdPymtAllowed = true;
        })
        .catch(function(error) {
          console.log("Error making payment of " + amount + " to " + address + ". ", error);
          fwdPymtAllowed = true;
        });
    });
}

function sleep(ms) {
  let timeToReturn = Date.now() + ms;
  while (Date.now() < timeToReturn) { }
}

function makePayments(client, totalToSend, payments) {
  client.price(function(err, price) {
    client.initWallet({
      identifier: process.env.WALLET_NAME,
      readOnly: true
    }, function(err, wallet) {
          if (err) {
              console.log('initWallet ERR', err);
              throw err;
          }

          wallet.getBalance()
          .then(function(value) {
              console.log("Wallet balance:", value[0]);
              if (value[0] > totalToSend) {
                console.log('Okay to forward funds.');
              } else {
                throw new Error("Insufficient wallet balance, " + value[0]);
              }
              return value;
          })
          .then(function(value) {
              for (let i=0; i < payments.length; i++) {
                  setTimeout(sendTransaction, 1000+(i*5000), wallet, payments[i].address, payments[i].amount, price);
              }
          })
          .catch(function(ex) {
            console.log('Exception', ex);
          });
    })
  });
}

function pricePerDollar(price) {
    let btcPrice = price.USD;
    let satoshisPerDollar = blocktrail.toSatoshi(1/btcPrice);
    return satoshisPerDollar;
}


function updateForwardedStatus(address) {
    let updateSql = "UPDATE PaymentAddress SET forwarded = 1 WHERE destinationAddress = ?";
    let updateStmt = payprocDb.prepare(updateSql);
    updateStmt.run(address, function(error) {
      if (error) {
        console.log("Error updating forwarded for address, " + address + ".");
      }
    });
    updateStmt.finalize();
}


payprocDb.all(query, function(err, rows) {
  let sumToFwd = new Map();
  let payAddrs = new Map();
  let payments = new Array(); // element = payment{address: 'xxxx', amount: 9999 }

  let client = blocktrail.BlocktrailSDK({
      apiKey : process.env.API_KEY,
      apiSecret : process.env.API_SECRET,
      testnet : false
  });
  let totalsum = 0;
  
  for (let i=0; i < rows.length; i++) {

    let payAddr = {
      destinationAddress: rows[i].destinationAddress,
      paaymentAddress: rows[i].paymentAddress,
      targetBalance: rows[i].targetBalance,
      payableBalance: rows[i].payableBalance,
      status: rows[i].status,
      forwarded: rows[i].forwarded
    }
    payAddrs.set(payAddr.paymentAddress, payAddr);

    if (!sumToFwd.get(rows[i].destinationAddress)) {
      sumToFwd.set(rows[i].destinationAddress, 0);
    }

    sumToFwd.set(rows[i].destinationAddress, sumToFwd.get(rows[i].destinationAddress)+rows[i].balance)
  }

  for (let [key, value] of sumToFwd) {
    console.log("Adding payment sum " + value + " to " + key);
    payments.push({address: key, amount: value});
    totalsum += value;
  }
  console.log("Total forwarding sum:", totalsum);

  makePayments(client, totalsum, payments);
});
