// src/components/auth/Login.jsx
import React, { useState } from "react";
import {
  Box,
  Container,
  Grid,
  TextField,
  Button,
  Typography,
  Link,
  Alert,
  InputAdornment,
  IconButton,
  Divider,
  Checkbox,
  FormControlLabel,
  CircularProgress,
  useTheme,
  alpha,
  Stack,
  Paper,
  styled,
} from "@mui/material";
import {
  Visibility,
  VisibilityOff,
  Email,
  Lock,
  LoginOutlined,
  Google,
  Facebook,
  Apple,
  Security,
  VideoCall,
} from "@mui/icons-material";
import { useAuth } from "../../hooks/useAuth";
import { useNavigate, useLocation } from "react-router-dom";
import { useEffect } from "react";
const StyledTextField = styled(TextField)(({ theme }) => ({
  "& .MuiOutlinedInput-root": {
    borderRadius: 12,
    backgroundColor: alpha(theme.palette.background.paper, 0.9),
    transition: "all 0.3s ease",
    height: 56,
    "&:hover": {
      backgroundColor: theme.palette.background.paper,
      "& .MuiOutlinedInput-notchedOutline": {
        borderColor: alpha(theme.palette.primary.main, 0.5),
      },
    },
    "&.Mui-focused": {
      backgroundColor: theme.palette.background.paper,
      boxShadow: `0 0 0 3px ${alpha(theme.palette.primary.main, 0.1)}`,
      "& .MuiOutlinedInput-notchedOutline": {
        borderColor: theme.palette.primary.main,
        borderWidth: 2,
      },
    },
  },
  "& .MuiInputLabel-root": {
    fontWeight: 500,
  },
}));

const SocialButton = styled(Button)(({ theme }) => ({
  borderRadius: 12,
  textTransform: "none",
  padding: "12px 24px",
  borderColor: alpha(theme.palette.divider, 0.3),
  backgroundColor: alpha(theme.palette.background.paper, 0.9),
  fontWeight: 600,
  fontSize: "1rem",
  transition: "all 0.3s ease",
  "&:hover": {
    borderColor: theme.palette.primary.main,
    backgroundColor: alpha(theme.palette.primary.main, 0.05),
    transform: "translateY(-2px)",
    boxShadow: "0 8px 24px rgba(0, 0, 0, 0.1)",
  },
}));

const Login = () => {
  const theme = useTheme();
  const navigate = useNavigate();
  const location = useLocation();
  const { login, loading } = useAuth();

  const [formData, setFormData] = useState({
    email: "",
    password: "",
    rememberMe: false,
  });

  const [showPassword, setShowPassword] = useState(false);
  const [errors, setErrors] = useState({});
  const [apiError, setApiError] = useState("");
  const [successMessage, setSuccessMessage] = useState("");
  useEffect(() => {
    if (location.state?.registrationSuccess) {
      setSuccessMessage("Registration successful! Please login to continue.");

      // Pre-fill email if provided
      if (location.state?.email) {
        setFormData((prev) => ({ ...prev, email: location.state.email }));
      }

      // Clear the navigation state
      window.history.replaceState({}, document.title);
    }
  }, [location]);
  const handleChange = (e) => {
    const { name, value, checked, type } = e.target;
    setFormData((prev) => ({
      ...prev,
      [name]: type === "checkbox" ? checked : value,
    }));

    if (errors[name]) {
      setErrors((prev) => ({ ...prev, [name]: "" }));
    }
  };

  const validateForm = () => {
    const newErrors = {};

    if (!formData.email) {
      newErrors.email = "Email is required";
    } else if (!/\S+@\S+\.\S+/.test(formData.email)) {
      newErrors.email = "Please enter a valid email address";
    }

    if (!formData.password) {
      newErrors.password = "Password is required";
    } else if (formData.password.length < 6) {
      newErrors.password = "Password must be at least 6 characters";
    }

    setErrors(newErrors);
    return Object.keys(newErrors).length === 0;
  };

  const handleSubmit = async (e) => {
    e.preventDefault();
    setApiError("");

    if (!validateForm()) return;

    try {
      await login(formData);
      navigate("/dashboard");
    } catch (error) {
      setApiError(error.message || "Login failed. Please try again.");
    }
  };

  const handleSocialLogin = (provider) => {
    console.log(`Login with ${provider}`);
  };

  return (
    <Box
      sx={{
        minHeight: "100vh",
        width: "100vw",
        position: "fixed",
        top: 0,
        left: 0,
        right: 0,
        bottom: 0,
        background: `linear-gradient(135deg, ${theme.palette.primary.main} 0%, ${theme.palette.secondary.main} 100%)`,
        "&::before": {
          content: '""',
          position: "absolute",
          top: 0,
          left: 0,
          right: 0,
          bottom: 0,
          background:
            'url("data:image/svg+xml,%3Csvg width="60" height="60" viewBox="0 0 60 60" xmlns="http://www.w3.org/2000/svg"%3E%3Cg fill="none" fill-rule="evenodd"%3E%3Cg fill="%23ffffff" fill-opacity="0.03"%3E%3Ccircle cx="30" cy="30" r="1.5"/%3E%3C/g%3E%3C/g%3E%3C/svg%3E")',
        },
        overflow: "auto",
      }}
    >
      <Container
        maxWidth="xl"
        sx={{ height: "100vh", display: "flex", alignItems: "center", py: 3 }}
      >
        <Grid container spacing={0} sx={{ height: "100%", maxHeight: "900px" }}>
          {/* Left Side - Branding */}
          <Grid item xs={12} md={7} lg={8}>
            <Box
              sx={{
                height: "100%",
                display: "flex",
                flexDirection: "column",
                justifyContent: "center",
                px: { xs: 2, md: 6, lg: 8 },
                color: "white",
                position: "relative",
                zIndex: 1,
              }}
            >
              {/* Logo */}
              <Stack direction="row" alignItems="center" spacing={2} mb={4}>
                <Box
                  sx={{
                    width: 60,
                    height: 60,
                    borderRadius: 2,
                    background: alpha(theme.palette.background.paper, 0.15),
                    backdropFilter: "blur(20px)",
                    display: "flex",
                    alignItems: "center",
                    justifyContent: "center",
                    border: `2px solid ${alpha(
                      theme.palette.common.white,
                      0.2
                    )}`,
                  }}
                >
                  <VideoCall sx={{ fontSize: 32, color: "white" }} />
                </Box>
                <Typography variant="h3" fontWeight={700}>
                  iMeetPro
                </Typography>
              </Stack>

              {/* Welcome Message */}
              <Typography
                variant="h2"
                fontWeight={700}
                mb={3}
                sx={{ lineHeight: 1.2 }}
              >
                Welcome Back to the Future of Meetings
              </Typography>

              <Typography
                variant="h6"
                sx={{ opacity: 0.9, mb: 4, maxWidth: 600 }}
              >
                Join millions of professionals who trust MeetingPro for secure,
                high-quality video conferencing and collaboration.
              </Typography>

              {/* Features */}
              <Stack spacing={2} mb={4}>
                {[
                  "Enterprise-grade security and encryption",
                  "Crystal clear HD video and audio quality",
                  "AI-powered meeting insights and analytics",
                  "Seamless integration with your workflow",
                ].map((feature, index) => (
                  <Stack
                    key={index}
                    direction="row"
                    alignItems="center"
                    spacing={2}
                  >
                    <Security
                      sx={{ color: alpha(theme.palette.common.white, 0.8) }}
                    />
                    <Typography variant="body1" sx={{ opacity: 0.9 }}>
                      {feature}
                    </Typography>
                  </Stack>
                ))}
              </Stack>

              {/* Stats */}
              <Grid container spacing={4} sx={{ mt: 2 }}>
                {[
                  { value: "10M+", label: "Active Users" },
                  { value: "99.9%", label: "Uptime" },
                  { value: "150+", label: "Countries" },
                ].map((stat, index) => (
                  <Grid item xs={4} key={index}>
                    <Typography variant="h4" fontWeight={700}>
                      {stat.value}
                    </Typography>
                    <Typography variant="body2" sx={{ opacity: 0.8 }}>
                      {stat.label}
                    </Typography>
                  </Grid>
                ))}
              </Grid>
            </Box>
          </Grid>

          {/* Right Side - Login Form */}
          <Grid item xs={12} md={5} lg={4}>
            <Box
              sx={{
                height: "100%",
                display: "flex",
                alignItems: "center",
                justifyContent: "center",
                px: { xs: 2, md: 4 },
                position: "relative",
                zIndex: 1,
              }}
            >
              <Paper
                elevation={0}
                sx={{
                  width: "100%",
                  maxWidth: 480,
                  p: { xs: 3, md: 4 },
                  borderRadius: 3,
                  background: alpha(theme.palette.background.paper, 0.95),
                  backdropFilter: "blur(20px)",
                  border: `1px solid ${alpha(theme.palette.common.white, 0.2)}`,
                  boxShadow: "0 32px 64px rgba(0, 0, 0, 0.15)",
                }}
              >
                {/* Header */}
                <Box textAlign="center" mb={4}>
                  <Typography
                    variant="h4"
                    fontWeight={700}
                    gutterBottom
                    sx={{
                      background: `linear-gradient(135deg, ${theme.palette.primary.main}, ${theme.palette.secondary.main})`,
                      backgroundClip: "text",
                      WebkitBackgroundClip: "text",
                      WebkitTextFillColor: "transparent",
                    }}
                  >
                    Welcome Back
                  </Typography>
                  <Typography variant="body1" color="text.secondary">
                    Sign in to continue to your account
                  </Typography>
                </Box>
                {successMessage && (
                  <Alert
                    severity="success"
                    sx={{
                      mb: 3,
                      borderRadius: 2,
                      "& .MuiAlert-message": {
                        fontWeight: 500,
                      },
                    }}
                    onClose={() => setSuccessMessage("")}
                  >
                    {successMessage}
                  </Alert>
                )}

                {/* Error Alert */}
                {apiError && (
                  <Alert
                    severity="error"
                    sx={{
                      mb: 3,
                      borderRadius: 2,
                      "& .MuiAlert-message": {
                        fontWeight: 500,
                      },
                    }}
                  >
                    {apiError}
                  </Alert>
                )}

                {/* Login Form */}
                <Box component="form" onSubmit={handleSubmit}>
                  <StyledTextField
                    fullWidth
                    name="email"
                    label="Email Address"
                    type="email"
                    value={formData.email}
                    onChange={handleChange}
                    error={!!errors.email}
                    helperText={errors.email}
                    InputProps={{
                      startAdornment: (
                        <InputAdornment position="start">
                          <Email color="action" />
                        </InputAdornment>
                      ),
                    }}
                    sx={{ mb: 3 }}
                  />

                  <StyledTextField
                    fullWidth
                    name="password"
                    label="Password"
                    type={showPassword ? "text" : "password"}
                    value={formData.password}
                    onChange={handleChange}
                    error={!!errors.password}
                    helperText={errors.password}
                    InputProps={{
                      startAdornment: (
                        <InputAdornment position="start">
                          <Lock color="action" />
                        </InputAdornment>
                      ),
                      endAdornment: (
                        <InputAdornment position="end">
                          <IconButton
                            onClick={() => setShowPassword(!showPassword)}
                            edge="end"
                          >
                            {showPassword ? <VisibilityOff /> : <Visibility />}
                          </IconButton>
                        </InputAdornment>
                      ),
                    }}
                    sx={{ mb: 3 }}
                  />

                  {/* Remember Me & Forgot Password */}
                  <Stack
                    direction="row"
                    justifyContent="space-between"
                    alignItems="center"
                    mb={3}
                  >
                    <FormControlLabel
                      control={
                        <Checkbox
                          name="rememberMe"
                          checked={formData.rememberMe}
                          onChange={handleChange}
                          color="primary"
                        />
                      }
                      label={
                        <Typography variant="body2" fontWeight={500}>
                          Remember me
                        </Typography>
                      }
                    />
                    <Link
                      component="button"
                      type="button"
                      variant="body2"
                      onClick={() => navigate("/auth/forgot-password")}
                      sx={{
                        textDecoration: "none",
                        fontWeight: 600,
                        color: theme.palette.primary.main,
                        "&:hover": {
                          textDecoration: "underline",
                        },
                      }}
                    >
                      Forgot Password?
                    </Link>
                  </Stack>

                  {/* Login Button */}
                  <Button
                    type="submit"
                    fullWidth
                    variant="contained"
                    size="large"
                    disabled={loading}
                    startIcon={
                      loading ? (
                        <CircularProgress size={20} />
                      ) : (
                        <LoginOutlined />
                      )
                    }
                    sx={{
                      py: 1.8,
                      borderRadius: 2,
                      textTransform: "none",
                      fontSize: "1.1rem",
                      fontWeight: 600,
                      background: `linear-gradient(135deg, ${theme.palette.primary.main}, ${theme.palette.secondary.main})`,
                      boxShadow: "0 8px 24px rgba(0, 0, 0, 0.15)",
                      mb: 3,
                      "&:hover": {
                        background: `linear-gradient(135deg, ${theme.palette.primary.dark}, ${theme.palette.secondary.dark})`,
                        transform: "translateY(-2px)",
                        boxShadow: "0 12px 32px rgba(0, 0, 0, 0.25)",
                      },
                    }}
                  >
                    {loading ? "Signing In..." : "Sign In to Account"}
                  </Button>

                  {/* Sign Up Link */}
                  <Box textAlign="center">
                    <Typography variant="body2" color="text.secondary">
                      New to iMeetPro?{" "}
                      <Link
                        component="button"
                        type="button"
                        variant="body2"
                        onClick={() => navigate("/auth/register")}
                        sx={{
                          textDecoration: "none",
                          fontWeight: 600,
                          color: theme.palette.primary.main,
                          "&:hover": {
                            textDecoration: "underline",
                          },
                        }}
                      >
                        Create your account
                      </Link>
                    </Typography>
                  </Box>
                </Box>
              </Paper>
            </Box>
          </Grid>
        </Grid>
      </Container>
    </Box>
  );
};

export default Login;
