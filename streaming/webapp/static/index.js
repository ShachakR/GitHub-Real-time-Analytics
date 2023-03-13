window.onload = function () {
    loadData();

    // Call the function every 60 seconds (60000 milliseconds)
    setInterval(loadData, 60000);
}

function loadData() {
    var xhr = new XMLHttpRequest();
    xhr.open('GET', '/getData', true);
    xhr.setRequestHeader('Content-Type', 'application/json');
    xhr.onreadystatechange = function () {
        if (xhr.readyState === 4 && xhr.status === 200) {
            console.log(xhr.responseText);
            
            if(xhr.responseText == "no data"){
                return;
            }

            var responseData = JSON.parse(xhr.responseText);

            let req1 = responseData['req1'];
            // Loop through the JSON list for requirement 1
            for (var i = 0; i < req1.length; i++) {
                let data = req1[i];
                document.getElementById(`language${i}`).innerHTML = `${data['language']}: ${data['count']}`
            }


        }
    };
    xhr.send();
}