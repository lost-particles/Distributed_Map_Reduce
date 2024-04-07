const mr = function(config) {
  let context = {};
  context.gid = config.gid || 'all';

  return {
    exec: (configuration, callback) => {
      /* Change this with your own exciting Map Reduce code! */
      console.log('Inside mr');
      let remote = {service: 'mapperReducer', method: 'map'};
      let message = [configuration['map'], configuration['keys'], context.gid];
      distribution[context.gid].comm.send(message, remote, (e, v)=>{
        console.log('After sending the msg');
        remote = {service: 'mapperReducer', method: 'shuffle'};
        message = [context.gid];
        distribution[context.gid].comm.send(message, remote, (e, v)=>{
          console.log('After all node shuffling has completed');
          remote = {service: 'mapperReducer', method: 'reduce'};
          message = [configuration['reduce'], context.gid];
          distribution[context.gid].comm.send(message, remote, (e, v)=>{
            console.log('All nodes have finished their reducing phase');
            let finalOut = [];
            Object.entries(v).forEach(([k, v])=>{
              finalOut.push(...v);
            });
            console.log(finalOut);
            callback(null, finalOut);
          });
        });
      });

      // callback(null, []);
    },
  };
};

module.exports = mr;
