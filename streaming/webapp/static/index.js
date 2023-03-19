var UPDATE_INTERVAL = 60

var timeLeft = UPDATE_INTERVAL; // Set initial time

window.onload = function () {
    loadData();
    startUpdateCycle();
}

function fetchUpdatedImage(src, id) {
    var img = document.getElementById(id);
    img.setAttribute("src", src);
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

            let req1 = responseData['req1'];
            // Loop through the JSON list for requirement 1
            for (var i = 0; i < req1.length; i++) {
                let data = req1[i];
                let id = "req1-language" + (i + 1)
                document.getElementById(id).innerHTML = `${data['language']}: ${data['count']}`
            }

            fetchUpdatedImage(responseData['req2'] + "?t=" + new Date().getTime(), "req2-plot");
            fetchUpdatedImage(responseData['req3'] + "?t=" + new Date().getTime(), "req3-plot");

            let req4 = responseData['req4'];
            // Loop through the JSON list for requirement 1
            for (var i = 0; i < req4.length; i++) {
                let data = req4[i];
                let id = "req4-language" + (i + 1)
                let result = `${data['language']}: <br>`
      
                for( var j = 0; j < data['top_ten_words'].length; j++){
                    let word_count = data['top_ten_words'][j]
                    result += `${word_count[0]}, ${word_count[1]} <br>`
                }

                document.getElementById(id).innerHTML = result
            }

        }
    };
    xhr.send();
}

 function startUpdateCycle() {
    countdownTimer = setInterval(function() {
        timeLeft--;
        document.getElementById("countdown").innerHTML = "Updating Data in: " + timeLeft + " seconds";
        if (timeLeft <= 0) {
            clearInterval(countdownTimer);
            timeLeft = UPDATE_INTERVAL;
            loadData();
            setTimeout(startCountdown, 1000);
        }
    }, 1000);
}