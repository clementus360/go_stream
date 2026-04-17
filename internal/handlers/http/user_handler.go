package http

import (
	"encoding/json"
	"net/http"
	"strconv"

	pb_user "github.com/clementus360/go_stream/internal/proto/user"
	"github.com/clementus360/go_stream/internal/service"
	"google.golang.org/grpc/metadata"
)

type UserHTTPHandler struct {
	Service *service.StreamManager
}

func NewUserHTTPHandler(s *service.StreamManager) *UserHTTPHandler {
	return &UserHTTPHandler{Service: s}
}

// UpdateProfilePicture handles PUT /v1/users/{user_id}/profile-picture
func (h *UserHTTPHandler) UpdateProfilePicture(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPut && r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Extract user_id from URL path, e.g., /users/123/profile-picture
	userIDStr := r.URL.Query().Get("user_id")
	if userIDStr == "" {
		http.Error(w, "Missing user_id", http.StatusBadRequest)
		return
	}

	userID, err := strconv.ParseInt(userIDStr, 10, 64)
	if err != nil {
		http.Error(w, "Invalid user_id", http.StatusBadRequest)
		return
	}

	// 1. Extract Token from HTTP Header
	authHeader := r.Header.Get("Authorization")
	if authHeader == "" {
		http.Error(w, "Missing Authorization header", http.StatusUnauthorized)
		return
	}

	// 2. Wrap the token in gRPC Metadata
	md := metadata.Pairs("authorization", authHeader)
	ctx := metadata.NewIncomingContext(r.Context(), md)

	// 3. Decode Body
	var req struct {
		ProfileImageURL string `json:"profile_image_url"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	if req.ProfileImageURL == "" {
		http.Error(w, "profile_image_url is required", http.StatusBadRequest)
		return
	}

	// 4. Call the User Service
	_, err = h.Service.UserClient.UpdateUserProfilePicture(ctx, &pb_user.UpdateUserProfilePictureRequest{
		UserId:          userID,
		ProfileImageUrl: req.ProfileImageURL,
	})
	if err != nil {
		http.Error(w, "Failed to update profile picture: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{
		"message": "Profile picture updated successfully",
	})
}

// GetUser handles GET /v1/users/{user_id}
func (h *UserHTTPHandler) GetUser(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	userIDStr := r.URL.Query().Get("user_id")
	if userIDStr == "" {
		http.Error(w, "Missing user_id", http.StatusBadRequest)
		return
	}

	userID, err := strconv.ParseInt(userIDStr, 10, 64)
	if err != nil {
		http.Error(w, "Invalid user_id", http.StatusBadRequest)
		return
	}

	resp, err := h.Service.UserClient.GetUserByID(r.Context(), &pb_user.GetUserByIDRequest{
		UserId: userID,
	})
	if err != nil {
		http.Error(w, "Failed to fetch user: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp.Profile)
}
