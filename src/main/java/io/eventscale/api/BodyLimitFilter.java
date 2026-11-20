package io.eventscale.api;

import io.eventscale.events.Contracts;
import jakarta.servlet.*;
import jakarta.servlet.http.*;
import java.io.*;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

@Component
public class BodyLimitFilter extends OncePerRequestFilter {
    @Override protected void doFilterInternal(HttpServletRequest request,HttpServletResponse response,FilterChain chain) throws ServletException,IOException {
        if (!request.getMethod().equals("POST") || !request.getRequestURI().equals("/api/v1/events")) { chain.doFilter(request,response); return; }
        byte[] bytes=request.getInputStream().readNBytes(Contracts.MAX_BYTES+1);
        if (bytes.length>Contracts.MAX_BYTES) { response.sendError(413,"Event exceeds 64 KiB"); return; }
        chain.doFilter(new HttpServletRequestWrapper(request) {
            @Override public ServletInputStream getInputStream() {
                var input=new ByteArrayInputStream(bytes);
                return new ServletInputStream() {
                    public int read() { return input.read(); }
                    public boolean isFinished() { return input.available()==0; }
                    public boolean isReady() { return true; }
                    public void setReadListener(ReadListener listener) { throw new UnsupportedOperationException(); }
                };
            }
            @Override public BufferedReader getReader() { return new BufferedReader(new InputStreamReader(getInputStream(),java.nio.charset.StandardCharsets.UTF_8)); }
        },response);
    }
}
