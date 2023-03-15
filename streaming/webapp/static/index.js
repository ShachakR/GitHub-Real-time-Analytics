var timeLeft = 60; // Set initial time
window.onload = function () {
    loadData();
    startCountdown();
    // Call the function every 60 seconds (60000 milliseconds)
}

function loadData() {
    var xhr = new XMLHttpRequest();
    xhr.open('GET', '/getData', true);
    xhr.setRequestHeader('Content-Type', 'application/json');
    xhr.onreadystatechange = function () {
        if (xhr.readyState === 4 && xhr.status === 200) {
            var responseData = JSON.parse(xhr.responseText);
            console.log(responseData)

            if(xhr.responseText["msg"] == "no data"){
                return;
            }

            let req1 = JSON.parse(responseData['req1']);
            // Loop through the JSON list for requirement 1
            for (var i = 0; i < req1.length; i++) {
                let data = req1[i];
                let id = "language" + (i + 1)
                document.getElementById(id).innerHTML = `${data['language']}: ${data['count']}`
            }


        }
    };
    xhr.send();
}

 // Function to start the countdown timer
 function startCountdown() {
    countdownTimer = setInterval(function() {
        timeLeft--;
        document.getElementById("countdown").innerHTML = "Updating Data in: " + timeLeft + " seconds";
        if (timeLeft <= 0) {
            clearInterval(countdownTimer);
            timeLeft = 60;
            loadData();
            setTimeout(startCountdown, 1000);
        }
    }, 1000);
}