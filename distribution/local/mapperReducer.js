const util = require('../util/util');
const store = require('../local/store');
const mem = require('../local/mem');
const id = require('../util/id');
const localGroups = require('../local/groups');
const localComm = require('../local/comm');

const mapperReducer = {
  map: function(mapFunc, keys, gid, callback) {
    console.log('mapper is: '+util.serialize(mapFunc));
    let resCount = 0;
    const recordCount = keys.length;
    const mapperOut = [];
    keys.forEach((ele)=>{
      store.get({gid: gid, key: ele}, (e2, v2)=>{
        if (v2) {
          mapperOut.push(mapFunc(ele, v2));
        }
        resCount++;
        if (resCount === recordCount) {
          if (gid === 'ncdc') {
            this.compact(mapperOut, (e, v)=>{
              mem.put(v, {gid: gid, key: 'mapOut'}, (e, v)=>{
                callback(null, 'done');
              });
            });
          } else {
            this.compactV2(mapperOut, (e, v)=>{
              mem.put(v, {gid: gid, key: 'mapOut'}, (e, v)=>{
                callback(null, 'done');
              });
            });
          }
        }
      });
    });
  },

  compact: function(mapperOut, callback) {
    console.log('before compaction : '+ JSON.stringify(mapperOut));
    const compactedOut = {};
    mapperOut.forEach((obj)=>{
      let objKey = Object.keys(obj)[0];
      if (objKey in compactedOut) {
        compactedOut[objKey].push(obj[objKey]);
      } else {
        compactedOut[objKey] = [obj[objKey]];
      }
    });
    console.log('After Compaction : '+ JSON.stringify(compactedOut));
    callback(null, compactedOut);
  },

  compactV2: function(mapperOut, callback) {
    const compactedOut = {};
    mapperOut.forEach((arr)=>{
      arr.forEach((ele)=>{
        Object.entries(ele).forEach(([k, v])=>{
          if (k in compactedOut) {
            compactedOut[k].push(v);
          } else {
            compactedOut[k] = [v];
          }
        });
      });
    });
    callback(null, compactedOut);
  },

  shuffle: function(gid, callback) {
    localGroups.get(gid, (e, v)=>{
      const nodeHashes = new Map();
      const nids =[];
      if (e && Object.keys(e).length !== 0) {
        callback(e, null);
      } else {
        Object.keys(v).forEach((sid)=>{
          nids.push(id.getID(v[sid]));
          nodeHashes.set(id.getID(v[sid]), v[sid]);
        });

        mem.get({gid: gid, key: 'mapOut'}, (e, v)=>{
          const recordCount = Object.keys(v).length;
          let responseCount = 0;

          Object.entries(v).forEach(([k, v])=>{
            let targetNid = id.consistentHash(id.getID(k), nids);
            let remote = {node: nodeHashes.get(targetNid),
              service: 'mapperReducer', method: 'shuffledSave'};
            let obj = {};
            obj[k] = v;
            const message = [obj, gid];
            localComm.send(message, remote, (e, v)=>{
              responseCount++;
              if (responseCount === recordCount) {
                mem.del({gid: gid, key: 'mapOut'}, (e, v)=>{
                  console.log('All shuffled record sent');
                  callback(e, v);
                });
              }
            });
          });
        });
      }
    });
  },

  shuffledSave: function(obj, gid, callback) {
    const key = Object.keys(obj)[0];
    mem.get({gid: gid, key: key}, (e, v)=>{
      let combinedArray;
      if (v) {
        combinedArray = [...v, ...obj[key]];
      } else {
        combinedArray = obj[key];
      }
      mem.put(combinedArray, {gid: gid, key: key}, (e, v)=>{
        callback(e, v);
      });
    });
  },

  reduce: function(reducer, gid, callback) {
    mem.get({gid: gid, key: null}, (e, v)=>{
      const recordCount = v.length;
      console.log('record count is : '+ recordCount);
      let responseCount = 0;
      if (recordCount === 0) {
        callback(null, []);
      }
      if (v) {
        const output = [];
        v.forEach((ele)=>{
          mem.get({gid: gid, key: ele}, (e, v)=>{
            if (v) {
              output.push(reducer(ele, v));
            }
            responseCount++;
            if (recordCount === responseCount) {
              callback(null, output);
            }
          });
        });
      }
    });
  },
};

module.exports = mapperReducer;
