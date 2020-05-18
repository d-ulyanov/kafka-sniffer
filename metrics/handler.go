package metrics

import "net/http"

type Handler struct {
	promHandler http.Handler
}

func NewHandler(promHandler http.Handler) *Handler {
	return &Handler{promHandler: promHandler}
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	h.promHandler.ServeHTTP(w, req)
}
