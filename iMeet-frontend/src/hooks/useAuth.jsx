// src/hooks/useAuth.js
import { useAuthContext, AuthProvider } from '../context/AuthContext';

export const useAuth = () => {
  return useAuthContext();
};

// Re-export AuthProvider for convenience
export { AuthProvider };