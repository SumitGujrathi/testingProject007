const { spawn } = require("child_process");
const express = require("express");
// const spawn = require("child_process").spawn;


const app = express();


app.get("/python", callName);

app.get("/check", function(req,res){
    res.send("Hi there this is testing check...!!!")
});


function callName(req, res){
    // var spawn = require("child_process").spawn;

    var process = spawn("python", ["./main.py"])

    process.stdout.on("data", function (data){
        res.send(data.toString());
    });
};


// app.listen(3000, function(){
//     console.log("running")
// })


