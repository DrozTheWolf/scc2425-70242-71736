package tukano.impl.data;

import java.util.Objects;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;

@Entity
public class Likes {

	@Id
	String id;

	String userId;

	String shortId;

	public String getId() { return id; }
	
	public String getOwnerId() {
		return ownerId;
	}

	public void setOwnerId(String ownerId) {
		this.ownerId = ownerId;
	}

	String ownerId;
	
	public Likes() {}

	public Likes(String userId, String shortId, String ownerId) {
		this.id = userId + "-" + shortId;
		this.userId = userId;
		this.shortId = shortId;
		this.ownerId = ownerId;
	}

	public void setLikesId(String id) {
		this.id = id;
	}

	public String getUserId() {
		return userId;
	}

	public void setUserId(String userId) {
		this.userId = userId;
	}

	public String getShortId() {
		return shortId;
	}

	public void setShortId(String shortId) {
		this.shortId = shortId;
	}

	@Override
	public String toString() {
		return "Likes [userId=" + userId + ", shortId=" + shortId + ", ownerId=" + ownerId + "]";
	}

	@Override
	public int hashCode() {
		return Objects.hash(ownerId, shortId, userId);
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Likes other = (Likes) obj;
		return Objects.equals(ownerId, other.ownerId) && Objects.equals(shortId, other.shortId)
				&& Objects.equals(userId, other.userId);
	}

}
