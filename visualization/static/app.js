var source = new EventSource('/stream');

var maxWords = 10;

var pdfData = [];

// example object
var dummyDataStruct = [
  {
  "title" : "bookTitle",
  "words" : [
    "word1",  "word2"
  ],
  "counts" : [
    10, 5
  ],
  "minCount" : 0
  }
];

function getBookTitle(event){
  return event.data.split("|")[0];
}

function getWord(event){
  return event.data.split("|")[1];
}

function getCount(event){
  return Number(event.data.split("|")[2]);
}

function isBookStored(bookTitle){
  for(var index = 0; index < pdfData.length ; index++){
    if(pdfData[index]['title'] === bookTitle){
      return true;
    }
  }
  return false;
}

function initializeBook(bookTitle){
  var bookObject = {};
  bookObject['title'] = bookTitle;
  bookObject['words'] = [];
  bookObject['counts'] = [];
  bookObject['minCount'] = Number(0);
  return bookObject;
}


function retrieveBookIndex(bookTitle){
  for(var index = 0; index < pdfData.length ; index++){
    if(pdfData[index]['title'] === bookTitle){
      return index;
    }
  }
  return -1;
}

function computeNewMin(bookContent){
  return Array.min(bookContent['counts']);
}

Array.min = function( array ){
    return Math.min.apply( Math, array );
};

source.onmessage = function (event) {
  bookTitle = getBookTitle(event);
  word = getWord(event);
  count = getCount(event);

  var bookIndex = retrieveBookIndex(bookTitle);

  // clear empty messages from REDIS
  if(typeof word === undefined  || count < 0){
    return;
  }

  // initialize new books
  if(bookIndex === -1){
    pdfData.push(initializeBook(bookTitle));
    bookIndex = retrieveBookIndex(bookTitle);
  }

  console.log('Current min log: '+ pdfData[bookIndex]['minCount']);

  // add word to book (if worth min count)
  if( pdfData[bookIndex]['minCount'] <= count){
    // is the word already contained
    if(!pdfData[bookIndex]['words'].includes(word)){
      // word not contained

      // iterate words and locate
      if(pdfData[bookIndex]['counts'].length === 0){
        pdfData[bookIndex]['words'].splice(0, 0, word);
        pdfData[bookIndex]['counts'].splice(0, 0, count);
      } else {
        for( var index = 0; index < pdfData[bookIndex]['counts'].length ; index++){
          if( pdfData[bookIndex]['counts'][index] < count){
            pdfData[bookIndex]['words'].splice(index, 0, word);
            pdfData[bookIndex]['counts'].splice(index, 0, count);
            break;
          }
        }
      }

      // remove extra words
      pdfData[bookIndex]['words'] = pdfData[bookIndex]['words'].slice(0, maxWords);
      pdfData[bookIndex]['counts'] = pdfData[bookIndex]['counts'].slice(0, maxWords);

    } else {
      // word contained

      // update current count
      var wordIndex = pdfData[bookIndex]['words'].indexOf(word);
      pdfData[bookIndex]['counts'][wordIndex] = count;
    }

    // update min count
      pdfData[bookIndex]['minCount'] = computeNewMin(pdfData[bookIndex]);
  }


};

var updateBookInformation = function () {
  // update tables with information
  var html = '';

  for (var index = 0 ; index < pdfData.length ; index++) {
      // create the new table per book
      if(pdfData[index]['words'].length > 0){
        var table = createTableForBook(pdfData[index]);
        html += table;
      }
  }


  document.getElementById("data").innerHTML = html;

};

function createTableForBook(bookData){
  var table =
   '<table class="table" style="text-align: center;"> '+
  '   <thead> '+
  '     <tr> '+
  '       <th class="text-center">' + bookData['title'] + '</th> '+
  '     </tr>'+
  '   </thead>'+
  '   <tbody>';

  var tableBody = '';
  for(var index = 0; index < bookData['words'].length ; index++){
      tableBody +=  '     <tr>  '+
                    '      <td>' + bookData['words'][index] + '</td>   '+
                    '      <td>' + bookData['counts'][index] + '</td>  '+
                    '     </tr>';
  }
  table += tableBody;

  table +=  '   </tbody> '+
            '</table>';
  return table;
}

//update display every #1000 milliseconds
window.setInterval(updateBookInformation, 1000);

//clean list, can be added to word skipping bolt
var skipList = ["https","follow","1","2","please","following","followers","fucking","RT","the","at","a"];

var skip = function(tWord){
  for(var i=0; i<skipList.length; i++){
    if(tWord === skipList[i]){
      return true;
    }
  }
  return false;
};
