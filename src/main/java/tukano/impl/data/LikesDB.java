package tukano.impl.data;

import tukano.impl.data.Likes;

public class LikesDB extends Likes {

    private String _rid;
    private String _ts;

    public LikesDB() {
    }

    public String get_rid() {
        return _rid;
    }

    public void set_rid(String _rid) {
        this._rid = _rid;
    }

    public String get_ts() {
        return _ts;
    }

    public void set_ts(String _ts) {
        this._ts = _ts;
    }


}
