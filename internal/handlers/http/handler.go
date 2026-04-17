package http

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	pb "github.com/clementus360/go_stream/internal/proto/stream_service"
	"github.com/clementus360/go_stream/internal/service"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type StreamHTTPHandler struct {
	Service *service.StreamManager
}

func NewStreamHTTPHandler(s *service.StreamManager) *StreamHTTPHandler {
	return &StreamHTTPHandler{Service: s}
}

func (h *StreamHTTPHandler) GetLiveStreams(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Parse limit from query params
	limitStr := r.URL.Query().Get("limit")
	limit, _ := strconv.Atoi(limitStr)
	if limit <= 0 {
		limit = 20
	}

	resp, err := h.Service.GetLiveStreams(r.Context(), &pb.GetLiveStreamsRequest{
		Limit: int32(limit),
	})
	if err != nil {
		http.Error(w, "Failed to fetch streams", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func (h *StreamHTTPHandler) GetStream(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Extract identifier from URL, e.g., /v1/streams/alice
	vars := strings.TrimPrefix(r.URL.Path, "/v1/streams/")
	if vars == "" {
		http.Error(w, "missing stream identifier", http.StatusBadRequest)
		return
	}

	fmt.Printf("Fetching stream with identifier: %s\n", vars)

	resp, err := h.Service.GetStream(r.Context(), &pb.GetStreamRequest{
		Identifier: vars,
	})

	if err != nil {
		st, _ := status.FromError(err)
		if st.Code() == codes.NotFound {
			http.Error(w, "Stream not found", http.StatusNotFound)
		} else {
			http.Error(w, "Internal error", http.StatusInternalServerError)
		}
		return
	}

	fmt.Printf("Stream found: %s (ID: %d)\n", resp.Title, resp.SessionId)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func (h *StreamHTTPHandler) UpdateMetadata(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost && r.Method != http.MethodPut {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// 1. Extract Token from HTTP Header
	authHeader := r.Header.Get("Authorization")
	if authHeader == "" {
		http.Error(w, "Missing Authorization header", http.StatusUnauthorized)
		return
	}

	// 2. Wrap the token in gRPC Metadata
	// This is the "Bridge": it makes the HTTP call look like a gRPC call to your service
	md := metadata.Pairs("authorization", authHeader)
	ctx := metadata.NewIncomingContext(r.Context(), md)

	// 3. Decode Body
	var req pb.UpdateMetadataRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	// 4. Call the Service Logic
	// Your Service logic will now find the metadata in the context just like before!
	resp, err := h.Service.UpdateStreamMetadata(ctx, &req)
	if err != nil {
		// Map gRPC errors to HTTP status codes
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}
