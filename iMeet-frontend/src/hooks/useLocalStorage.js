// hooks/useLocalStorage.js
import { useState, useEffect } from 'react';

export const useLocalStorage = (key, initialValue) => {
  // Get initial value from localStorage or use provided initial value
  const [storedValue, setStoredValue] = useState(() => {
    try {
      const item = window.localStorage.getItem(key);
      if (item === null) {
        return initialValue;
      }
      
      // Try to parse as JSON, but handle cases where localStorage contains plain strings
      try {
        return JSON.parse(item);
      } catch (parseError) {
        // If JSON parsing fails, check if it's a plain string value
        console.warn(`localStorage key "${key}" contains invalid JSON, attempting to handle as plain string:`, item);
        
        // Remove surrounding quotes if they exist and treat as string
        const cleanedValue = item.replace(/^"|"$/g, '');
        
        // Store it back properly as JSON for future use
        window.localStorage.setItem(key, JSON.stringify(cleanedValue));
        
        return cleanedValue;
      }
    } catch (error) {
      console.error(`Error reading localStorage key "${key}":`, error);
      return initialValue;
    }
  });

  // Update localStorage when state changes
  const setValue = (value) => {
    try {
      // Allow value to be a function so we have the same API as useState
      const valueToStore = value instanceof Function ? value(storedValue) : value;
      setStoredValue(valueToStore);
      window.localStorage.setItem(key, JSON.stringify(valueToStore));
    } catch (error) {
      console.error(`Error setting localStorage key "${key}":`, error);
    }
  };

  // Remove item from localStorage
  const removeValue = () => {
    try {
      window.localStorage.removeItem(key);
      setStoredValue(initialValue);
    } catch (error) {
      console.error(`Error removing localStorage key "${key}":`, error);
    }
  };

  return [storedValue, setValue, removeValue];
};