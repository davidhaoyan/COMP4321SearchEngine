document.getElementById("search-button").addEventListener("click", () => {
    fetchResults();
})

document.body.addEventListener("keypress", (e) => {
    if (e.key === 'Enter') {
        fetchResults();
    }
})


function fetchResults() {
    fetch("http://localhost:8080/search", {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json'
        },
        body: JSON.stringify({query: document.getElementById("search-input").value} )
    })
        .then(response => {
            if (!response.ok) {
                alert("Couldn't reach the server. Please check if it is being hosted at http://localhost:8080");
            }
            return response.json();
        })
        .then(data => {
            console.log(data);
            showResults();
            for (let n = 0; n < 50; n++) {
                constructEntry(n, data);
            }

        })
        .catch(error => {
            console.error(error);
            alert("Error parsing JSON response. Please check queries are spelled correctly");
        });
}
function showResults() {
    let searchContainer = document.getElementById("search-container");
    searchContainer.style.top = "0";
    searchContainer.style.left = "0";
    searchContainer.style.transform = "translate(0, 0)";
    searchContainer.style.flexDirection = "row";
    searchContainer.style.position = "relative";
    let searchResults = document.getElementById("search-results");
    searchResults.innerHTML = "";
    document.getElementById("results-container").style.display = "flex";
}

function constructEntry(n, data) {
    let entry = document.createElement("div");
    let id = "entry#" + n;
    entry.id = id;
    entry.className = "entry";
    let entryScore = document.createElement("div");
    entryScore.className = "entry-score";
    entryScore.append(extractData(n, data, "score"));

    let entryInfo = document.createElement("div");
    entryInfo.className = "entry-info"
    entryInfo.append(extractData(n, data, "title"));
    entryInfo.append(extractData(n, data, "url"));
    entryInfo.append(extractData(n, data, "lastDateModified"));
    entryInfo.append(extractData(n, data, "keywords"));
    entryInfo.append(extractData(n, data, "parents"));
    entryInfo.append(extractData(n, data, "children"));

    entry.append(entryScore);
    entry.append(entryInfo);
    document.getElementById("search-results").append(entry);
}

function extractData(n, data, attribute) {
    let e = document.createElement("span");
    switch (attribute) {
        case "score":
            e.innerHTML = "score: " + parseFloat(data[n][attribute]).toFixed(2);
            break;
        case "title":
            e.innerHTML = "<a href=" + data[n]["url"] + ">" + data[n][attribute] + "</a>";
            break;
        case "url":
            e.innerHTML = "<a href=" + data[n][attribute] + ">" + data[n][attribute] + "</a>";
            break;
        case "lastDateModified":
            e.innerHTML = data[n][attribute] + ", size:" + data[n]["size"];
            break;
        case "keywords":
            e.innerHTML = "keywords: {";
            for (let key in data[n][attribute]) {
                e.innerHTML += key + ": " + data[n][attribute][key] + ", ";
            }
            e.innerHTML += "}"
            break;
        case "parents":
            for (let key in data[n][attribute]) {
                e.innerHTML += "Parent <a href=" + data[n][attribute][key] + ">" + data[n][attribute][key] + "</a><br>";
            }
            break;
        case "children":
            for (let key in data[n][attribute]) {
                e.innerHTML += "Child <a href=" + data[n][attribute][key] + ">" + data[n][attribute][key] + "</a><br>";
            }
            break;
    }
    return e;
}