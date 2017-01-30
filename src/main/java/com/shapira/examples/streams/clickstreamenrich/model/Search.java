package com.shapira.examples.streams.clickstreamenrich.model;

/**
 * Created by gwen on 1/28/17.
 */
public class Search {
    int userID;
    String searchTerms;

    public Search(int userID, String searchTerms) {
        this.userID = userID;
        this.searchTerms = searchTerms;
    }

    public int getUserID() {
        return userID;
    }

    public String getSearchTerms() {
        return searchTerms;
    }
}
