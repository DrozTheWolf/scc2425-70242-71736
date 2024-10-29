package tukano.api;

import tukano.api.Short;

public class ShortDB extends Short {

    private String _rid;
    private String _ts;

    public ShortDB() {
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
