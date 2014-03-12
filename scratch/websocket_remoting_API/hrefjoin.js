//the only change needed 

// this



URL.prototype.join = function(href) {
    // implement http://www.ietf.org/rfc/rfc1808.txt
    // which says, in BNF, that:
    // an absolute URL is just something prefixed by "scheme:" where 'scheme' is actually http or https or ftp or ws or data or whatever; in particular, an absolute URL can be scheme:relativeURL
    
    // so relative URLs are king
    // an a relative URL is either an absolute path (which is identified simply by the fact that it starts with a /) a net_path URL (those funny //google.com/site URLs) which is identified by it starting with two slashes, and everything else is relative
    // a URL can come in absolute and relative flavoures
    if (href.startsWith("//")) { //net_path, section 2.4.3
        return new URL(this.protocol + href); //notice how the spec writers cleverly made href already contain the two needed slashes here
    } 
    if(href.startsWith("/")) { //absolute path
        return new URL(this.origin + href);
    } else { //absolute and relative URLs
      //we else-this because I'm abusing exceptions 
     //the URL class demands an absolute URL, so if it crashes we know the URL is relative
     try { //absolute URL
     	return new URL(href);
     } catch //relative URL
     {
        //section "4. Resolving Relative URLS"
        // ...huh. accroding to rfc1808, a relative URL is supposed to inherit the query string if it doesn't provide one
        //and same for query params... tho
        //but by experimenting with what seems the be this algorithm in browsers, via location.assign, the params and querystring in the original URL are totally ignored
        
        path = this.pathname.split("/");
        path.pop() //remove final component (possibly empty)
        // XXX corner-case: http://site.com/folder vs http://site.com/folder/ do relative hrefs from  know that folder is a folder? testing this will require writing a server that fakes URL
        // aha, thank you IIS and your durpiness:
        // this site provides a test platform:
        // http://weblogs.asp.net/durp.jpg/
        //try running location.assign("hello/") a few times, then
        //            location.assign("hello2") and location.assign("hello3"); observe that hello/ keeps ~appending~, but hello3 replaces hello2 because hello2 didn't end with a /
        //so indeed, RELATIVE URLS MAY DIFFER. SO http://site.com/topiclist is a DIFFERENT page than http://site.com/topiclist/
        return new URL(this.origin + path.join("/") + href);
    }
    
}

function websockethref(href) {
	U = new URL(window.location.href).join(href)	
    // now, map it to a websocket
    if(U.protocol == "https:") { U.protocol = "wss" }
    else if(U.protocol == "http:") { U.protocol = "ws"}
    else { throw new URIError(U.href + " is not an HTTP URL") }
    //case III should be impossible because if window.location exists we're running in a browser, and if we're running in a browser we're over HTTP
    //but better safe-than-sorry
}
