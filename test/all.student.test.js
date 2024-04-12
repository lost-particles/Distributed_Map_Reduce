global.nodeConfig = {ip: '127.0.0.1', port: 7070};
const distribution = require('../distribution');
const id = distribution.util.id;

const groupsTemplate = require('../distribution/all/groups');

const ncdcGroup = {};
const dlibGroup = {};


/*
   This hack is necessary since we can not
   gracefully stop the local listening node.
   The process that node is
   running in is the actual jest process
*/
let localServer = null;

/*
    The local node will be the orchestrator.
*/

const n1 = {ip: '127.0.0.1', port: 7110};
const n2 = {ip: '127.0.0.1', port: 7111};
const n3 = {ip: '127.0.0.1', port: 7112};

beforeAll((done) => {
  /* Stop the nodes if they are running */

  ncdcGroup[id.getSID(n1)] = n1;
  ncdcGroup[id.getSID(n2)] = n2;
  ncdcGroup[id.getSID(n3)] = n3;

  dlibGroup[id.getSID(n1)] = n1;
  dlibGroup[id.getSID(n2)] = n2;
  dlibGroup[id.getSID(n3)] = n3;

  const startNodes = (cb) => {
    distribution.local.status.spawn(n1, (e, v) => {
      distribution.local.status.spawn(n2, (e, v) => {
        distribution.local.status.spawn(n3, (e, v) => {
          cb();
        });
      });
    });
  };

  distribution.node.start((server) => {
    localServer = server;

    const ncdcConfig = {gid: 'ncdc'};
    startNodes(() => {
      groupsTemplate(ncdcConfig).put(ncdcConfig, ncdcGroup, (e, v) => {
        const dlibConfig = {gid: 'dlib'};
        groupsTemplate(dlibConfig).put(dlibConfig, dlibGroup, (e, v) => {
          done();
        });
      });
    });
  });
});

afterAll((done) => {
  let remote = {service: 'status', method: 'stop'};
  remote.node = n1;
  distribution.local.comm.send([], remote, (e, v) => {
    remote.node = n2;
    distribution.local.comm.send([], remote, (e, v) => {
      remote.node = n3;
      distribution.local.comm.send([], remote, (e, v) => {
        localServer.close();
        done();
      });
    });
  });
});

function sanityCheck(mapper, reducer, dataset, expected, done) {
  let mapped = dataset.map((o) =>
    mapper(Object.keys(o)[0], o[Object.keys(o)[0]]));
  /* Flatten the array. */
  mapped = mapped.flat();
  let shuffled = mapped.reduce((a, b) => {
    let key = Object.keys(b)[0];
    if (a[key] === undefined) a[key] = [];
    a[key].push(b[key]);
    return a;
  }, {});
  let reduced = Object.keys(shuffled).map((k) => reducer(k, shuffled[k]));

  try {
    expect(reduced).toEqual(expect.arrayContaining(expected));
  } catch (e) {
    done(e);
  }
}

// ---all.mr---

test('(25 pts) all.mr:ncdc/ lowest', (done) => {
  let m1 = (key, value) => {
    let words = value.split(/(\s+)/).filter((e) => e !== ' ');
    console.log(words);
    let out = {};
    out[words[1]] = parseInt(words[3]);
    return out;
  };

  let r1 = (key, values) => {
    let out = {};
    out[key] = values.reduce((a, b) => Math.min(a, b), Infinity);
    return out;
  };

  let dataset = [
    {'000': '006701199099999 1950 0515070049999999N9 +0000 1+9999'},
    {'106': '004301199099999 1950 0515120049999999N9 +0022 1+9999'},
    {'212': '004301199099999 1950 0515180049999999N9 -0011 1+9999'},
    {'318': '004301265099999 1949 0324120040500001N9 +0111 1+9999'},
    {'424': '004301265099999 1949 0324180040500001N9 +0078 1+9999'},
  ];

  let expected = [{'1950': -11}, {'1949': 78}];

  /* Sanity check: map and reduce locally */
  sanityCheck(m1, r1, dataset, expected, done);

  /* Now we do the same thing but on the cluster */
  const doMapReduce = (cb) => {
    distribution.ncdc.store.get(null, (e, v) => {
      try {
        expect(v.length).toBe(dataset.length);
      } catch (e) {
        done(e);
      }


      distribution.ncdc.mr.exec({keys: v, map: m1, reduce: r1}, (e, v) => {
        try {
          expect(v).toEqual(expect.arrayContaining(expected));
          done();
        } catch (e) {
          done(e);
        }
      });
    });
  };

  let cntr = 0;

  // We send the dataset to the cluster
  dataset.forEach((o) => {
    let key = Object.keys(o)[0];
    let value = o[key];
    distribution.ncdc.store.put(value, key, (e, v) => {
      cntr++;
      // Once we are done, run the map reduce
      if (cntr === dataset.length) {
        doMapReduce();
      }
    });
  });
});

test('(25 pts) all.mr:ncdc/ same temp', (done) => {
  let m1 = (key, value) => {
    let words = value.split(/(\s+)/).filter((e) => e !== ' ');
    console.log(words);
    let out = {};
    out[words[1]] = parseInt(words[3]);
    return out;
  };

  let r1 = (key, values) => {
    let out = {};
    out[key] = values.reduce((a, b) => Math.min(a, b), Infinity);
    return out;
  };

  let dataset = [
    {'000': '006701199099999 1950 0515070049999999N9 +0011 1+9999'},
    {'106': '004301199099999 1950 0515120049999999N9 +0011 1+9999'},
    {'212': '004301199099999 1950 0515180049999999N9 +0011 1+9999'},
    {'318': '004301265099999 1949 0324120040500001N9 +0111 1+9999'},
    {'424': '004301265099999 1949 0324180040500001N9 +0011 1+9999'},
  ];

  let expected = [{'1950': 11}, {'1949': 11}];

  /* Sanity check: map and reduce locally */
  sanityCheck(m1, r1, dataset, expected, done);

  /* Now we do the same thing but on the cluster */
  const doMapReduce = (cb) => {
    distribution.ncdc.store.get(null, (e, v) => {
      try {
        expect(v.length).toBe(dataset.length);
      } catch (e) {
        done(e);
      }


      distribution.ncdc.mr.exec({keys: v, map: m1, reduce: r1}, (e, v) => {
        try {
          expect(v).toEqual(expect.arrayContaining(expected));
          done();
        } catch (e) {
          done(e);
        }
      });
    });
  };

  let cntr = 0;

  // We send the dataset to the cluster
  dataset.forEach((o) => {
    let key = Object.keys(o)[0];
    let value = o[key];
    distribution.ncdc.store.put(value, key, (e, v) => {
      cntr++;
      // Once we are done, run the map reduce
      if (cntr === dataset.length) {
        doMapReduce();
      }
    });
  });
});

test('(25 pts) all.mr:ncdc/ Duplicate key', (done) => {
  let m1 = (key, value) => {
    let words = value.split(/(\s+)/).filter((e) => e !== ' ');
    console.log(words);
    let out = {};
    out[words[1]] = parseInt(words[3]);
    return out;
  };

  let r1 = (key, values) => {
    let out = {};
    out[key] = values.reduce((a, b) => Math.min(a, b), Infinity);
    return out;
  };

  let dataset = [
    {'000': '006701199099999 1950 0515070049999999N9 +0011 1+9999'},
    {'319': '004301199099999 1950 0515120049999999N9 +0011 1+9999'},
    {'212': '004301199099999 1950 0515180049999999N9 +0011 1+9999'},
    {'325': '004301265099999 1949 0324120040500001N9 -0011 1+9999'},
    {'319': '004301265099999 1949 0324180040500001N9 +0015 1+9999'},
  ];

  let expected = [{'1950': 11}, {'1949': -11}];

  /* Sanity check: map and reduce locally */
  sanityCheck(m1, r1, dataset, expected, done);

  /* Now we do the same thing but on the cluster */
  const doMapReduce = (cb) => {
    distribution.ncdc.store.get(null, (e, v) => {
      try {
        expect(v.length).toBe(dataset.length);
      } catch (e) {
        done(e);
      }


      distribution.ncdc.mr.exec({keys: v, map: m1, reduce: r1}, (e, v) => {
        try {
          expect(v).toEqual(expect.arrayContaining(expected));
          done();
        } catch (e) {
          done(e);
        }
      });
    });
  };

  let cntr = 0;

  // We send the dataset to the cluster
  dataset.forEach((o) => {
    let key = Object.keys(o)[0];
    let value = o[key];
    distribution.ncdc.store.put(value, key, (e, v) => {
      cntr++;
      // Once we are done, run the map reduce
      if (cntr === dataset.length) {
        doMapReduce();
      }
    });
  });
});

test('(25 pts) all.mr:dlib / extra data', (done) => {
  let m2 = (key, value) => {
    // map each word to a key-value pair like {word: 1}
    let words = value.split(/(\s+)/).filter((e) => e !== ' ');
    let out = [];
    words.forEach((w) => {
      let o = {};
      o[w] = 1;
      out.push(o);
    });
    return out;
  };

  let r2 = (key, values) => {
    let out = {};
    out[key] = values.length;
    return out;
  };

  let dataset = [
    {'b1-l1': 'It was the best of times, it was the worst of times,'},
    {'b1-l2': 'it was the age of wisdom, it was the age of foolishness,'},
    {'b1-l3': 'it was the epoch of belief, it was the epoch of incredulity,'},
    {'b1-l4': 'it was the season of Light, it was the season of Darkness,'},
    {'b1-l5': 'it was the spring of hope, it was the winter of despair,'},
    {'b1-l6': 'it was the best'},
  ];

  let expected = [
    {It: 1}, {was: 11},
    {the: 11}, {best: 2},
    {of: 10}, {'times,': 2},
    {it: 10}, {worst: 1},
    {age: 2}, {'wisdom,': 1},
    {'foolishness,': 1}, {epoch: 2},
    {'belief,': 1}, {'incredulity,': 1},
    {season: 2}, {'Light,': 1},
    {'Darkness,': 1}, {spring: 1},
    {'hope,': 1}, {winter: 1},
    {'despair,': 1},
  ];

  /* Sanity check: map and reduce locally */
  sanityCheck(m2, r2, dataset, expected, done);

  /* Now we do the same thing but on the cluster */
  const doMapReduce = (cb) => {
    distribution.dlib.store.get(null, (e, v) => {
      try {
        expect(v.length).toBe(dataset.length);
      } catch (e) {
        done(e);
      }

      distribution.dlib.mr.exec({keys: v, map: m2, reduce: r2}, (e, v) => {
        try {
          expect(v).toEqual(expect.arrayContaining(expected));
          done();
        } catch (e) {
          done(e);
        }
      });
    });
  };

  let cntr = 0;

  // We send the dataset to the cluster
  dataset.forEach((o) => {
    let key = Object.keys(o)[0];
    let value = o[key];
    distribution.dlib.store.put(value, key, (e, v) => {
      cntr++;
      // Once we are done, run the map reduce
      if (cntr === dataset.length) {
        doMapReduce();
      }
    });
  });
});

test('(25 pts) all.mr:dlib/ case insensitive', (done) => {
  let m2 = (key, value) => {
    // map each word to a key-value pair like {word: 1}
    let words = value.toLowerCase().split(/(\s+)/).filter((e) => e !== ' ');
    let out = [];
    words.forEach((w) => {
      let o = {};
      o[w] = 1;
      out.push(o);
    });
    return out;
  };

  let r2 = (key, values) => {
    let out = {};
    out[key] = values.length;
    return out;
  };

  let dataset = [
    {'b1-l1': 'It was the best of times, it was the worst of times,'},
    {'b1-l2': 'it was the age of wisdom, it was the age of foolishness,'},
    {'b1-l3': 'it was the epoch of belief, it was the epoch of incredulity,'},
    {'b1-l4': 'it was the season of Light, it was the season of Darkness,'},
    {'b1-l5': 'it was the spring of hope, it was the winter of despair,'},
    {'b1-l6': 'it was the best'},
  ];

  let expected = [
    {was: 11},
    {the: 11}, {best: 2},
    {of: 10}, {'times,': 2},
    {it: 11}, {worst: 1},
    {age: 2}, {'wisdom,': 1},
    {'foolishness,': 1}, {epoch: 2},
    {'belief,': 1}, {'incredulity,': 1},
    {season: 2}, {'light,': 1},
    {'darkness,': 1}, {spring: 1},
    {'hope,': 1}, {winter: 1},
    {'despair,': 1},
  ];

  /* Sanity check: map and reduce locally */
  sanityCheck(m2, r2, dataset, expected, done);

  /* Now we do the same thing but on the cluster */
  const doMapReduce = (cb) => {
    distribution.dlib.store.get(null, (e, v) => {
      try {
        expect(v.length).toBe(dataset.length);
      } catch (e) {
        done(e);
      }

      distribution.dlib.mr.exec({keys: v, map: m2, reduce: r2}, (e, v) => {
        try {
          expect(v).toEqual(expect.arrayContaining(expected));
          done();
        } catch (e) {
          done(e);
        }
      });
    });
  };

  let cntr = 0;

  // We send the dataset to the cluster
  dataset.forEach((o) => {
    let key = Object.keys(o)[0];
    let value = o[key];
    distribution.dlib.store.put(value, key, (e, v) => {
      cntr++;
      // Once we are done, run the map reduce
      if (cntr === dataset.length) {
        doMapReduce();
      }
    });
  });
});


test('(0 pts) sample test', () => {
  const t = true;
  expect(t).toBe(true);
});
test('(0 pts) sample test', () => {
  const t = true;
  expect(t).toBe(true);
});
test('(0 pts) sample test', () => {
  const t = true;
  expect(t).toBe(true);
});
test('(0 pts) sample test', () => {
  const t = true;
  expect(t).toBe(true);
});
test('(0 pts) sample test', () => {
  const t = true;
  expect(t).toBe(true);
});
