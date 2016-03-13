package cs535.subredditrecommender.utilities;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class RatingsInformationClass implements Writable {

    private String SubRedditID;
    private Double RatingperUserandSubredditID;
    private Double RatingsperUser;


    public RatingsInformationClass() {
    }

    public void setRatingsperUserandSubredditID(Double val) {
        this.RatingperUserandSubredditID = val;
    }

    public String getSubRedditID() {
        return this.SubRedditID;
    }

    public void setSubRedditID(String subredditID) {
        this.SubRedditID = subredditID;
    }

    public Double getRatingperUserandSubredditID() {
        return this.RatingperUserandSubredditID;
    }

    public Double getRatingsperUser() {
        return this.RatingsperUser;
    }

    public void setRatingsperUser(Double val) {
        this.RatingsperUser = val;
    }

    @Override
    public void readFields(DataInput din) throws IOException {
        // TODO Auto-generated method stub
        SubRedditID = din.readUTF();
        RatingperUserandSubredditID = din.readDouble();
        RatingsperUser = din.readDouble();
    }

    @Override
    public void write(DataOutput dout) throws IOException {
        // TODO Auto-generated method stub
        dout.writeUTF(SubRedditID);
        dout.writeDouble(RatingperUserandSubredditID);
        dout.writeDouble(RatingsperUser);
    }

}
