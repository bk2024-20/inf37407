from django.shortcuts import render
# En prod, tu peux laisser l’accueil public (sans login_required)
def home_view(request):
    return render(request, "home.html")
