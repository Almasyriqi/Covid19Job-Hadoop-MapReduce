const express = require('express')
const app = express()
const port = 4000
const {execFile} = require('child_process')
const { NodeSSH } = require('node-ssh')
const password = '1234'
const ssh = new NodeSSH()

ssh.connect({
    host: '172.20.10.4',
    username: 'hadoopuser',
    port: 22,
    password,
    tryKeyboard: true,
}).then(() => {
    ssh.execCommand('pwd', { cwd: '/home/hadoopuser' }).then((result) => {
        console.log('STDOUT: ' + result.stdout);
        console.log('STDERR: ' + result.stderr);
    });
});

app.get('/', (request, response) => {
    response.send('Big Data Hadoop MapReduce')
})

app.get('/hadoop', (request, response) => {
    execFile('run-job.sh', {shell:true}, (stdout) => {
        console.log(stdout);
        ssh.execCommand('hadoop fs -cat /Riqi/Output/hasil/part-00000', { cwd: '/home/hadoopuser' }).then((result) => {
            console.log('STDOUT: ' + result.stdout);
            console.log('STDERR: ' + result.stderr);
            response.send(result.stdout);
        });
        ssh.execCommand('hadoop fs -rm -r /Riqi/Output/hasil/', { cwd: '/home/hadoopuser' }).then((result) => {
            console.log('STDOUT: ' + result.stdout);
            console.log('STDERR: ' + result.stderr);
        });
    });
})

app.listen(port, () => {
    console.log(`Listen app backend on port ${port}`)
})
