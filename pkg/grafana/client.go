package grafana

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"time"

	"github.com/pkg/errors"
	"github.com/valyala/fasthttp"
)

type AuthParams struct {
	User       string
	Password   string
	APIToken   string
	AuthCookie string
}

func (p *AuthParams) Validate() error {
	var i int
	if p.User != "" {
		i++
	}
	if p.APIToken != "" {
		i++
	}
	if p.AuthCookie != "" {
		i++
	}

	if i > 1 {
		return errors.New("only one authentication method can be specified (user/password, API token or auth cookie")
	}

	if i == 0 {
		return errors.New("missing authentication credentials. API token, cookie or user/password should be provided.")
	}

	return nil
}

func NewClient(httpC *fasthttp.Client, params AuthParams) (*Client, error) {

	if err := params.Validate(); err != nil {
		return nil, err
	}

	return &Client{
		client:     httpC,
		user:       params.User,
		password:   params.Password,
		token:      params.APIToken,
		authCookie: params.AuthCookie,
	}, nil
}

type Client struct {
	client     *fasthttp.Client
	authCookie string
	token      string
	user       string
	password   string
}

const AuthCookieName = "grafana_session"

func (c *Client) Do(req *fasthttp.Request) (*fasthttp.Response, error) {
	c.setAuthHeaders(req)
	httpResp := fasthttp.AcquireResponse()
	err := c.client.Do(req, httpResp)
	return httpResp, errors.Wrap(err, "failed to make request in network client")
}

func (c *Client) DoWithTimeout(req *fasthttp.Request, timeout time.Duration) (*fasthttp.Response, error) {
	c.setAuthHeaders(req)
	httpResp := fasthttp.AcquireResponse()
	err := c.client.DoTimeout(req, httpResp, timeout)
	return httpResp, errors.Wrap(err, "failed to make request in network client")
}

func (c *Client) Post(url string) (int, []byte, error) {
	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)
	req.SetRequestURI(url)
	req.Header.SetMethod(fasthttp.MethodPost)
	httpResp, err := c.Do(req)
	defer fasthttp.ReleaseResponse(httpResp)
	if err != nil {
		return 0, nil, err
	}
	return httpResp.StatusCode(), httpResp.Body(), nil
}

func (c *Client) PostJSON(url string, reqBody interface{}) (int, []byte, error) {
	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)
	req.SetRequestURI(url)
	req.Header.SetMethod(fasthttp.MethodPost)

	req.Header.SetContentType("application/json")
	reqArgs, err := json.Marshal(reqBody)
	if err != nil {
		return 0, nil, errors.Wrap(err, "failed to marshal json body")
	}
	req.SetBody(reqArgs)

	httpResp, err := c.Do(req)
	defer fasthttp.ReleaseResponse(httpResp)
	if err != nil {
		return 0, nil, err
	}
	return httpResp.StatusCode(), httpResp.Body(), nil
}

func (c *Client) Get(url string) (int, []byte, error) {
	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)
	req.SetRequestURI(url)
	httpResp, err := c.Do(req)
	defer fasthttp.ReleaseResponse(httpResp)
	if err != nil {
		return 0, nil, err
	}
	return httpResp.StatusCode(), httpResp.Body(), err
}

func (c *Client) GetWithTimeout(url string, timeout time.Duration) (int, []byte, error) {
	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)
	req.SetRequestURI(url)
	httpResp, err := c.DoWithTimeout(req, timeout)
	defer fasthttp.ReleaseResponse(httpResp)
	if err != nil {
		return 0, nil, err
	}
	return httpResp.StatusCode(), httpResp.Body(), err
}

func (c *Client) setAuthHeaders(req *fasthttp.Request) {
	if c.user != "" {
		h := base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf("%s:%s", c.user, c.password)))
		req.Header.Set("Authorization", fmt.Sprintf("Basic %s", h))
	}

	if c.token != "" {
		req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", c.token))
	}

	if c.authCookie != "" {
		req.Header.SetCookie(AuthCookieName, c.authCookie)
	}
}
